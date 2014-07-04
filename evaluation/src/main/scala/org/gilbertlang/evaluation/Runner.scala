package org.gilbertlang.evaluation

import java.io._
import org.gilbertlang.language.Gilbert
import org.gilbertlang.optimizer.Optimizer
import org.gilbertlang.runtime.{withSpark, local, withStratosphere}
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

import collection.JavaConversions._

import org.ini4j.{Ini}

import scala.collection.mutable.ListBuffer
;

object Runner {
  val DEFAULT_JOBMANAGER = "node1.stsffap.org"
  val DEFAULT_JOBMANAGER_PORT = -1;
  val DEFAULT_JOBMANAGER_PORT_SPARK = 7077
  val DEFAULT_JOBMANAGER_PORT_STRATOSPHERE = 6123

  val data =  collection.mutable.HashMap[String, List[String]]()
  var outputFile: String = null
  var template: String = null
  var compilerHints: Boolean = false
  var densityThresholds: List[Double] = null
  var tries: Int = 0
  var optimizationTP: Boolean = false
  var optimizationMMReordering: Boolean = false
  var blocksizes: List[Int] = null
  var engine: Engines.Value = null
  var headerNames: List[String] = null
  var headerReferences: List[String] = null
  var parallelism: List[Int] = null
  var outputPath: String = null
  var checkpointDir: String = null
  var iterationUntilCheckpoint: Int = 0
  var jobmanager: String = ""
  var jobmanagerPort: Int = 0

  var datapoints: ListBuffer[DatapointEntry] = ListBuffer()

  def main(args: Array[String]){
    if(args.length < 1){
      printUsage
    }else {
      val file = new File("evaluation/src/main/resources/exampleConfig.evalution")
      val ini = new Ini(file)

      processData(ini)
      processConfiguration(ini)
      processHeader(ini)

      try {
        checkSettings
      } catch{
        case e: Exception =>
          throw new RuntimeException("The settings have not been set properly.", e)
      }

      runEvaluation
    }
  }

  def getBlocksizes: List[Int] = {
    if(headerReferences.contains("blocksize")){
      this.blocksizes
    }else{
      this.blocksizes.take(1)
    }
  }

  def getDensityThresholds: List[Double] = {
    if(headerReferences.contains("densityThreshold")){
      this.densityThresholds
    }else{
      this.densityThresholds.take(1)
    }
  }

  def getDataLength(dataReferences: Set[String]): Int = {
    var max = 1
    var firstElement = true

    for(reference <- dataReferences){
      if(!data.contains(reference)){
        throw new RuntimeException(s"Data with key $reference was not specified")
      }

      val length = data(reference).length

      if(!firstElement && max != length){
        throw new RuntimeException("Data elements don't have the same length")
      }

      if(firstElement){
        max = length
        firstElement = false
      }
    }

    max
  }

  def getDataSet(references: Set[String], idx: Int): Map[String, String] = {
    var result = Map[String, String]()

    val pairs = for(reference <- references) yield {
      (reference, data(reference)(idx))
    }

    val constants = for(key <- data.keySet if !references.contains(key))yield {
      (key, data(key)(0))
    }

    (constants ++ pairs).toMap
  }

  def runEvaluation{
    val dataReferences = headerReferences filter {
      element =>
        element match {
          case "densityThreshold" | "blocksize" | "time" | "error" => false
          case _ => true
        }
    } toSet

    val dataLength = getDataLength(dataReferences)

    for(dop <- parallelism) {
      for (blocksize <- getBlocksizes) {
        for (densityThreshold <- getDensityThresholds) {
          for (idx <- 0 until dataLength) {
            val dataSet = getDataSet(dataReferences, idx)
            val runtimeConfig = RuntimeConfiguration(blocksize, densityThreshold, compilerHints)
            val engineConfig = EngineConfiguration(dop, engine, jobmanager, jobmanagerPort, optimizationTP,
              optimizationMMReordering, tries, outputPath, checkpointDir, iterationUntilCheckpoint)

            val (time, error) = run(evaluationConfig, runtimeConfig, engineConfig, template, dataSet)

            addDatapoint(time, error, dop, blocksize, densityThreshold, dataSet)
          }
        }
      }
    }
  }

  def run(evaluationConfig: EvaluationConfiguration, runtimeConfig: RuntimeConfiguration,
          engineConfiguration: EngineConfiguration,
          template: String,
          dataSet: Map[String,
            String]) : (Double, Double) = {
    val program =  instantiateTemplate(new FileReader(new File(template)), dataSet)
    val executable = Gilbert.compileString(program)

    val postMMReordering = if(evaluation.optMMReordering){
      Optimizer.mmReorder(executable)
    }else{
      executable
    }

    val postTP = if(evaluation.optTP){
      Optimizer.transposePushdown(postMMReordering)
    }else{
      postMMReordering
    }

    val executor = evaluationConfig.engine match {
      case Engines.Local => local()
      case Engines.Spark => withSpark.remote(engineConfiguration)
      case Engines.Stratosphere => withStratosphere().remote(engineConfiguration)
    }


    val measurements:List[Double] = for(t <- 0 until tries) yield {
      executor.execute(postTP)
    }

    val average = measurements.fold(0)(_+_)/tries
    val std = if(tries > 1) math.sqrt(1/(tries-1)* measurements.fold(0)(_ + math.pow((_ - average),2))) else 0

    (average, std)
  }

  def instantiateTemplate(reader: Reader, dataSet: Map[String, String]): String = {
    val bufferedReader = new BufferedReader(reader)
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)

    var line = bufferedReader.readLine()

    val pattern = """\$(\w+)""".r

    while(line != null){
      val replacedLine = pattern.replaceAllIn(line, {
        m =>
          dataSet(m.group(1))
      })

      printWriter.println(replacedLine)
      line = bufferedReader.readLine()
    }

    stringWriter.toString
  }

  def addDatapoint(time: Double, error: Double, parallelism: Int, blocksize: Int, densityThreshold: Double,
                   dataSet: Map[String,
                     String]){
    datapoints += DatapointEntry(time, error, parallelism, blocksize, densityThreshold, dataSet)
  }

  def processData(ini: Ini){
    val dataSection = ini.get("data")

    val entries = dataSection.keySet()

    for(entry <- entries){
      val entryValues = dataSection.get(entry).split(",")
      data.put(entry, entryValues.toList)
    }
  }

  def processConfiguration(ini: Ini){
    val configSection = ini.get("config")

    setParallelism(configSection.get("parallelism").split(",").toList)
    this.jobmanager = configSection.get("jobmanager", DEFAULT_JOBMANAGER)
    this.jobmanagerPort = configSection.get("jobmanager.port", classOf[Int],DEFAULT_JOBMANAGER_PORT)
    setOutputPath(configSection.get("outputPath"))
    setCheckpointDir(configSection.get("checkpointDir"))
    setIterationUntilCheckpoint(configSection.get("iterationUntilCheckpoint"))
    setEngine(configSection.get("engine"))
    setCompilerHints(configSection.get("compilerHints"))
    setTemplate(configSection.get("template"))
    setBlocksize(configSection.get("blocksize").split(",").toList)
    setDensityThreshold(configSection.get("densityThreshold").split(",").toList)
    setOutputFile(configSection.get("output"))
    setTries(configSection.get("tries"))
    setOptMMReordering(configSection.get("optimization.MMReordering"))
    setOptTP(configSection.get("optimization.TP"))
  }

  def setParallelism(dops: List[String]){
    this.parallelism = dops map { value => value.toInt}
  }

  def setOutputPath(value: String){
    this.outputPath = if(value == null) "" else value
  }

  def setCheckpointDir(value: String){
    this.checkpointDir = if(value==null) "" else value
  }

  def setIterationUntilCheckpoint(value: String){
    this.iterationUntilCheckpoint = if(value == null) 0 else value.toInt
  }

  def setOptMMReordering(value: String){
    this.optimizationMMReordering = value.toBoolean
  }

  def setOptTP(value: String){
    this.optimizationTP = value.toBoolean
  }

  def setEngine(engineName: String) {
    engineName match {
      case "Stratosphere" => this.engine = Engines.Stratosphere
      case "Spark" => this.engine = Engines.Spark
      case "Local" => this.engine = Engines.Local
      case _ => throw new RuntimeException(s"$engineName is not supported")
    }
  }

  def setTemplate(template: String) {
    val file = new File(template)

    if(!file.exists()){
      throw new RuntimeException(s"File $template does not exist")
    }

    this.template = template
  }

  def setCompilerHints(value: String) {
    this.compilerHints = value.toBoolean
  }

  def setBlocksize(sizes: List[String]){
    this.blocksizes = sizes map { _.toInt }
  }

  def setDensityThreshold(thresholds: List[String]){
    this.densityThresholds = thresholds map { _.toDouble }
  }

  def setOutputFile(outputFile: String) {
    this.outputFile = outputFile
  }

  def setTries(triesStr: String) {
    this.tries = triesStr.toInt
  }

  def processHeader(ini: Ini){
    val section = ini.get("header")
    val headerEntries = section.get("header").split(" ")

    val splittedEntries = headerEntries map { entry => entry.split(":")}

    val (names, references) =splittedEntries.unzip{ array =>
      val result = (array(0), array(1))
      result
    }

    this.headerNames = names.toList
    this.headerReferences = references.toList
  }

  def checkSettings {
    if(outputFile == null){
      throw new RuntimeException("Output file is null")
    }

    if(template == null){
      throw new RuntimeException("Template file is null")
    }

    if(this.engine == null){
      throw new RuntimeException("No engine specified")
    }

    if(this.blocksizes == null){
      throw new RuntimeException("Blocksizes have not been specified")
    }

    if(this.densityThresholds == null){
      throw new RuntimeException("Density thresholds not set")
    }

    if(this.tries <= 0){
      throw new RuntimeException("Tries not set")
    }

    if(jobmanagerPort < 0){
      jobmanagerPort = engine match {
        case Engines.Stratosphere => DEFAULT_JOBMANAGER_PORT_STRATOSPHERE
        case Engines.Spark => DEFAULT_JOBMANAGER_PORT_SPARK
        case _ => -1
      }
    }
  }

  def printUsage {
    println("java org.gilbertlang.evaluation.Runner <configurationFile>")
  }

}
