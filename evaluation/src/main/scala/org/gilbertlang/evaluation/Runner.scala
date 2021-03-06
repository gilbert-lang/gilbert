package org.gilbertlang.evaluation

import java.io._
import org.apache.commons.io.FileUtils
import org.gilbertlang.language.Gilbert
import org.gilbertlang.optimizer.Optimizer
import org.gilbertlang.runtime._
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration
import scala.language.postfixOps

import collection.JavaConversions._

import org.ini4j.{Ini}

import scala.collection.mutable.ListBuffer


object Runner {
  val DEFAULT_JOBMANAGER = "cloud-11"
  val DEFAULT_JOBMANAGER_PORT = -1;
  val DEFAULT_JOBMANAGER_PORT_SPARK = 7077
  val DEFAULT_JOBMANAGER_PORT_STRATOSPHERE = 6123
  val DEFAULT_SEPARATOR = " "
  val DEFAULT_ENGINE = "Flink"
  val DEFAULT_MATHBACKEND = "Breeze"
  val DEFAULT_COMPILERHINTS = "true"
  val DEFAULT_BLOCKSIZE = "1000"
  val DEFAULT_DENSITYTHRESHOLD = "0.5"
  val DEFAULT_OUTPUT_FILE = "evaluationResult"
  val DEFAULT_OPTIMIZATION_MMREORDERING = "true"
  val DEFAULT_OPTIMIZATION_TP = "true"
  val DEFAULT_TRIES = "10"
  val DEFAULT_VERBOSE_WRITING = "false"
  val DEFAULT_MEMORY = "20g"

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
  var master: String = ""
  var port: Int = 0
  var appName: String = ""
  var mathBackend: MathBackend.Value = null
  var jars: List[String] = null
  var libraryPath: String = ""
  var verboseWriting: Boolean = false;
  var memory: String = null;

  var datapoints: ListBuffer[DatapointEntry] = ListBuffer()

  def main(args: Array[String]){
    if(args.length < 1){
      printUsage
    }else {
      val file = new File(args(0))
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

      runEvaluation()

      printOutput()
    }
  }

  def printOutput() {
    val file = new File(outputFile)
    if(file.exists() && file.isDirectory){
      FileUtils.deleteDirectory(file)
    }
    val printer = new PrintWriter(file)

    printInfo(printer)
    printHeader(printer, DEFAULT_SEPARATOR)
    printDatapoints(printer, DEFAULT_SEPARATOR)

    printer.close()
  }

  def printInfo(printer: PrintWriter){
    printer.println("# Engine: " + this.engine +"; Math-Backend: " + this.mathBackend)
    printer.println("# Template: " + this.template)
    printer.println("# App-Name: " + this.appName)
    printer.println("# Compiler Hints: " + this.compilerHints)
    printer.println("# Optimization: TP: " + this.optimizationTP + "; MM Reorder: " + this.optimizationMMReordering)
    printer.println("# tries: " + this.tries)
    printer.println("# checkpointDir: " + this.checkpointDir + ";" + this
      .iterationUntilCheckpoint)
    val configAdditional = "# " + (if(!headerReferences.contains("parallelism")) "DOP: " + this.parallelism(0) + " "
    else "") + (if (!headerReferences.contains("densityThreshold")) "DensityThreshold: "+ this.densityThresholds(0) +
      " "else "") + (if(!headerReferences.contains("blocksize")) "Blocksize: " + this.blocksizes(0) + " " else "")
    val additional = "# " + this.data.keys.filter{key => !headerReferences.contains(key)}.map{key => key + ": " +
      data(key) + ";"}.mkString(" ");

    if(configAdditional.length > 2){
      printer.println(configAdditional)
    }

    if(additional.length >2){
      printer.println(additional)
    }

    printer.println()
  }

  def printHeader(printer: PrintWriter, separator: String){
    for(header <- headerNames){
      printer.print(header + separator)
    }

    printer.println()
  }

  def printDatapoints(printer: PrintWriter, separator: String){
    for(datapoint <- datapoints){
      for(reference <- headerReferences){
        reference match {
          case "time" => printer.print(datapoint.time)
          case "error" => printer.print(datapoint.error)
          case _ => printer.print(datapoint.dataset.getOrElse(reference, "NotFound"))
        }
        printer.print(separator)
      }
      printer.println()
    }
  }

  def getParallelism(idx: Int): Int = {
    if(idx >= parallelism.length){
      parallelism.last
    }else{
      parallelism(idx)
    }
  }

  def getBlocksize(idx:Int): Int = {
    if(idx >= blocksizes.length){
      blocksizes.last
    }else{
      blocksizes(idx)
    }
  }

  def getDensityThreshold(idx: Int): Double = {
    if(idx >= densityThresholds.length){
      densityThresholds.last
    }else{
      densityThresholds(idx)
    }
  }

  def getDataLength(dataReferences: Set[String]): Int = {
    var max = 0
    var firstElement = true

    for(reference <- dataReferences){
      if(!data.contains(reference)){
        throw new RuntimeException(s"Data with key $reference was not specified")
      }

      val length = data(reference).length

      if(max < length){
        max = length
      }
    }

    max
  }

  def getDataSet(references: Set[String], idx: Int): Map[String, String] = {
    var result = Map[String, String]()

    val pairs = for(reference <- references) yield {
      val value = if(data(reference).length <= idx){
        data(reference).last
      }else{
        data(reference)(idx)
      }
      (reference, value)
    }

    val constants = for(key <- data.keySet if !references.contains(key))yield {
      (key, data(key)(0))
    }

    (constants ++ pairs).toMap
  }

  def runEvaluation(){
    val dataReferences = headerReferences filter {
      element =>
        element match {
          case "time" | "error" | "parallelism" | "densityThreshold" | "blocksize" => false
          case _ => true
        }
    } toSet

    val dataLength = (getDataLength(dataReferences)::parallelism.length::densityThresholds.length::List(blocksizes
      .length)).reduce(math.max(_,_))

    for (idx <- 0 until dataLength) {
      val dataSet = getDataSet(dataReferences, idx)
      val dop = getParallelism(idx)
      val blocksize = getBlocksize(idx)
      val densityThreshold = getDensityThreshold(idx)

      val runtimeConfig = RuntimeConfiguration(blocksize, densityThreshold, compilerHints,
        if(outputPath.isEmpty) None else Some(outputPath), if(checkpointDir.isEmpty) None else Some
          (checkpointDir),iterationUntilCheckpoint, verboseWriting)
      val engineConfig = EngineConfiguration(master, port, appName, dop, jars, libraryPath, Some(memory))
      val evaluationConfig = EvaluationConfiguration(this.engine, this.mathBackend, this.tries,
        this.optimizationMMReordering,
        this.optimizationTP)

      val (time, error) = run(evaluationConfig, runtimeConfig, engineConfig, template, dataSet)

      addDatapoint(time, error, dataSet.+(("parallelism",dop.toString)).+(("densityThreshold",
        densityThreshold.toString)).+(("blocksize", blocksize.toString)))
    }
  }

  def run(evaluationConfig: EvaluationConfiguration, runtimeConfig: RuntimeConfiguration,
          engineConfiguration: EngineConfiguration,
          template: String,
          dataSet: Map[String,
            String]) : (Double, Double) = {
    val program =  instantiateTemplate(new FileReader(new File(template)), dataSet)
    val executable = Gilbert.compileString(program)

    val postMMReordering = if(evaluationConfig.optMMReordering){
      println("Optimization: Matrix multiplication reordering.")
      Optimizer.mmReorder(executable)
    }else{
      executable
    }

    val postTP = if(evaluationConfig.optTP){
      println("Optimization: Transpose push down.")
      Optimizer.transposePushdown(postMMReordering)
    }else{
      postMMReordering
    }

    evaluationConfig.mathBackend match {
      case MathBackend.Mahout => withMahout()
      case MathBackend.Breeze => withBreeze()
    }

    val executor = evaluationConfig.engine match {
      case Engines.Local => local()
      case Engines.Spark => withSpark.remote(engineConfiguration)
      case Engines.Flink => withFlink.remote(engineConfiguration)
    }



    val measurements =
      for (t <- 0 until evaluationConfig.tries) yield {
        try {
          val t = executor.execute(postTP, runtimeConfig)
          executor.stop()
          t
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
            -1
        }
      }

    val cleanedMeasurements = measurements.filter{_ >= 0}
    val num = cleanedMeasurements.length
    val average = cleanedMeasurements.fold(0.0)(_+_)/num
    val std = if(num > 1){
      math.sqrt(1.0/(num-1)* cleanedMeasurements.
        foldLeft(0.0){ (s, e) => s + math.pow((e -average),2)})
    } else{
      0
    }

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

      printWriter.print(replacedLine)
      line = bufferedReader.readLine()

      if (line != null) {
        printWriter.println()
      }
    }

    stringWriter.toString
  }

  def addDatapoint(time: Double, error: Double, dataSet: Map[String,String]){
    datapoints += DatapointEntry(time, error, dataSet)
  }

  def processData(ini: Ini){
    val dataSection = ini.get("data")

    val entries = dataSection.keySet()

    for(entry <- entries){
      val entryValues = dataSection.get(entry).split(",").map{entry => entry.trim}
      data.put(entry, entryValues.toList)
    }
  }

  def processConfiguration(ini: Ini){
    val configSection = ini.get("config")

    setParallelism(configSection.get("parallelism", "1").split(",").toList)
    this.master = configSection.get("master", DEFAULT_JOBMANAGER)
    this.port = configSection.get("port", classOf[Int],DEFAULT_JOBMANAGER_PORT)
    this.appName = configSection.get("appName", "Evaluation")
    this.libraryPath = configSection.get("libraryPath", new File(this.getClass().getProtectionDomain().getCodeSource
      ().getLocation().getFile()).getParent()+"/lib/")
    setOutputPath(configSection.get("outputPath"))
    setCheckpointDir(configSection.get("checkpointDir"))
    setJars(configSection.get("jars"))
    setIterationUntilCheckpoint(configSection.get("iterationUntilCheckpoint"))
    setEngine(configSection.get("engine", DEFAULT_ENGINE))
    setMathBackend(configSection.get("math", DEFAULT_MATHBACKEND))
    setCompilerHints(configSection.get("compilerHints", DEFAULT_COMPILERHINTS))
    setTemplate(configSection.get("template"))
    setBlocksize(configSection.get("blocksize", DEFAULT_BLOCKSIZE).split(",").toList)
    setDensityThreshold(configSection.get("densityThreshold", DEFAULT_DENSITYTHRESHOLD).split(",").toList)
    setOutputFile(configSection.get("outputFile", DEFAULT_OUTPUT_FILE))
    setTries(configSection.get("tries", DEFAULT_TRIES))
    setOptMMReordering(configSection.get("optimization.MMReordering", DEFAULT_OPTIMIZATION_MMREORDERING))
    setOptTP(configSection.get("optimization.TP", DEFAULT_OPTIMIZATION_TP))
    this.verboseWriting = configSection.get("verboseWriting", DEFAULT_VERBOSE_WRITING).toBoolean
    this.memory = configSection.get("memory", DEFAULT_MEMORY)
  }

  def setJars(value: String){
    if(value == null){
      this.jars = List()
    }else{
      this.jars = value.split(",").toList
    }
  }

  def setMathBackend(value: String){
    value match {
      case "Mahout" => mathBackend = MathBackend.Mahout
      case "Breeze" => mathBackend = MathBackend.Breeze
      case _ => throw new RuntimeException(s"$value is not supported as math backend")
    }
  }

  def setParallelism(dops: List[String]){
    this.parallelism = dops map { value => value.trim.toInt}
  }

  def setOutputPath(value: String){
    this.outputPath = if(value == null) "" else value
  }

  def setCheckpointDir(value: String){
    this.checkpointDir = if(value==null) "" else value
  }

  def setIterationUntilCheckpoint(value: String){
    this.iterationUntilCheckpoint = if(value == null || value.isEmpty) 0 else value.toInt
  }

  def setOptMMReordering(value: String){
    this.optimizationMMReordering = value.toBoolean
  }

  def setOptTP(value: String){
    this.optimizationTP = value.toBoolean
  }

  def setEngine(engineName: String) {
    engineName match {
      case "Flink" => this.engine = Engines.Flink
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
    this.blocksizes = sizes map { _.trim.toInt }
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

    val (names, references) =splittedEntries.map{ array =>
      val result = (array(0), array(1))
      result
    }.unzip

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

    if(port < 0){
      port = engine match {
        case Engines.Flink => DEFAULT_JOBMANAGER_PORT_STRATOSPHERE
        case Engines.Spark => DEFAULT_JOBMANAGER_PORT_SPARK
        case _ => -1
      }
    }
  }

  def printUsage {
    println("java org.gilbertlang.evaluation.Runner <configurationFile>")
  }

}
