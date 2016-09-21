/**
 * gilbert - Distributed Linear Algebra on Sparse Matrices
 * Copyright (C) 2013  Sebastian Schelter, Till Rohrmann
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gilbertlang.runtime

import org.apache.log4j.{Level, Logger, SimpleLayout, WriterAppender}
import org.apache.spark.{SparkConf, SparkContext}
import org.gilbertlang.runtime.execution.reference.ReferenceExecutor
import org.gilbertlang.runtime.execution.spark.SparkExecutor
import org.gilbertlang.runtime.execution.flink.FlinkExecutor
import Executables._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.client.LocalExecutor
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.gilbertlang.runtimeMacros.linalg.{MatrixFactory, RuntimeConfiguration}
import org.gilbertlang.runtimeMacros.linalg.breeze.{BreezeBooleanMatrixFactory, BreezeDoubleMatrixFactory}
import org.gilbertlang.runtimeMacros.linalg.mahout.{MahoutBooleanMatrixFactory, MahoutDoubleMatrixFactory}

object local {
  class LocalExecutionEngine extends ExecutionEngine{
    val executor = new ReferenceExecutor()
    def stop() {}

    def execute(program: Executable, configuration: RuntimeConfiguration): Double = {
      val finalProgram = terminateExecutable(program)

      val start = System.nanoTime()

      executor.run(finalProgram, configuration)

      (System.nanoTime() - start) / 1e9
    }
  }

  def apply(): ExecutionEngine = new LocalExecutionEngine()
}

object withSpark {
  class SparkExecutionEngine(config: SparkConf) extends ExecutionEngine{
    var sc: SparkContext = null

    def stop(){
      if(sc != null){
        sc.stop
      }
    }

    def execute(program: Executable, runtimeConfig: RuntimeConfiguration): Double = {
      val finalProgram = terminateExecutable(program)

      val pattern = """Job \d+ finished: [^,]*, took ([0-9\.]*) s""".r
      val sparkLogger = Logger.getLogger("org.apache.spark.scheduler.DAGScheduler")
      sparkLogger.setLevel(Level.INFO)
      val gilbertTimer = new GilbertTimer(pattern)
      sparkLogger.addAppender(new WriterAppender(new SimpleLayout(), gilbertTimer))

      sc = new SparkContext(config)
      val sparkExecutor = new SparkExecutor(sc)
      runtimeConfig.checkpointDir foreach { dir => sparkExecutor.sc.setCheckpointDir(dir) }
      sparkExecutor.run(finalProgram, runtimeConfig)

      gilbertTimer.totalTime
    }
  }

  def local(engineConfiguration: EngineConfiguration): ExecutionEngine = {
    val master = "local[" + engineConfiguration.parallelism + "]"

    createSparkExecutionEngine(master, engineConfiguration)
  }

  def remote(engineConfiguration: EngineConfiguration): ExecutionEngine = {
    val master = "spark://" + engineConfiguration.master +":" + engineConfiguration.port
    val sparkDeps = getSparkDependencies(engineConfiguration.libraryPath)
    createSparkExecutionEngine(master, engineConfiguration.copy(jars = engineConfiguration.jars ++ sparkDeps))
  }

  def createSparkExecutionEngine(master: String, engineConfiguration: EngineConfiguration): ExecutionEngine = {
    val sparkConf = new SparkConf().
      setMaster(master).
      setAppName(engineConfiguration.appName).
      setJars(engineConfiguration.jars).
      set("spark.cores.max", engineConfiguration.parallelism.toString).
      set("spark.default.parallelism", engineConfiguration.parallelism.toString).
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    engineConfiguration.memory foreach { m => sparkConf.set("spark.executor.memory", m)}

    new SparkExecutionEngine(sparkConf)
  }

  def getSparkDependencies(root: String): List[String] = {
    val jars = List("runtime-0.1-SNAPSHOT.jar",
      "runtimeMacros-0.1-SNAPSHOT.jar",
      "mahout-math-0.12.2.jar",
      "flink-core-1.1.2.jar")

    jars map { jar => root + jar}
  }
}

object withFlink{
  class FlinkExecutionEngine(val env: ExecutionEnvironment, val appName: String) extends
  ExecutionEngine {
    val translator = new FlinkExecutor(env, appName)

    def stop() {}

    def execute(program: Executable, configuration: RuntimeConfiguration): Double = {
      val finalProgram = terminateExecutable(program)


      val config = configuration.copy(outputPath = Some(configuration.outputPath.getOrElse("file://" +
        System.getProperty("user.dir"))))

      translator.run(finalProgram, config)

      val result = env.execute(appName)

      val netTime= result.getNetRuntime
      netTime.toDouble/1000
    }
  }

  def local(engineConfiguration: EngineConfiguration): ExecutionEngine = {
    val executor = new LocalExecutor
    executor.setDefaultOverwriteFiles(true)

    val configuration = new Configuration()
    configuration.setBoolean(ConfigConstants.FILESYSTEM_DEFAULT_OVERWRITE_KEY, true);
    configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, engineConfiguration.parallelism)
    configuration.setInteger(ConfigConstants.DEFAULT_PARALLELISM_KEY, engineConfiguration.parallelism)

    val env = ExecutionEnvironment.createLocalEnvironment(configuration)

    new FlinkExecutionEngine(env, engineConfiguration.appName)
  }

  def remote(engineConfiguration: EngineConfiguration): ExecutionEngine = {

    val flinkDependencies: List[String] = getFlinkDependencies(engineConfiguration.libraryPath)

    val jarFiles = engineConfiguration.jars ++ flinkDependencies

    val env = ExecutionEnvironment.createRemoteEnvironment(
      engineConfiguration.master,
      engineConfiguration.port,
      engineConfiguration.parallelism,
      jarFiles: _*)

    new FlinkExecutionEngine(env, engineConfiguration.appName)
  }

  private def getFlinkDependencies(root: String): List[String] = {
    val jars = List("runtime-0.1-SNAPSHOT.jar", "runtimeMacros-0.1-SNAPSHOT.jar", "breeze_2.11-0.11.2.jar",
      "mahout-math-0.12.2.jar", "commons-math3-3.2.jar")

    jars map { jar => root + jar}
  }
}

object withMahout{
  def apply() {
    MatrixFactory.doubleMatrixFactory = MahoutDoubleMatrixFactory
    MatrixFactory.booleanMatrixFactory = MahoutBooleanMatrixFactory
  }
}

object withBreeze{
  def apply() {
    MatrixFactory.doubleMatrixFactory = BreezeDoubleMatrixFactory
    MatrixFactory.booleanMatrixFactory = BreezeBooleanMatrixFactory
  }
}
