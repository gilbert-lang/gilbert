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

import eu.stratosphere.api.common.PlanExecutor
import org.apache.log4j.{WriterAppender, SimpleLayout, Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.gilbertlang.runtime.execution.reference.ReferenceExecutor
import org.gilbertlang.runtime.execution.spark.SparkExecutor
import org.gilbertlang.runtime.execution.stratosphere.StratosphereExecutor
import eu.stratosphere.api.scala.ScalaPlan
import eu.stratosphere.api.scala.ScalaSink
import Executables._
import eu.stratosphere.client.{RemoteExecutor, LocalExecutor}
import org.gilbertlang.runtimeMacros.linalg.{RuntimeConfiguration, MatrixFactory}
import org.gilbertlang.runtimeMacros.linalg.breeze.{BreezeBooleanMatrixFactory, BreezeDoubleMatrixFactory}
import org.gilbertlang.runtimeMacros.linalg.mahout.{MahoutBooleanMatrixFactory, MahoutDoubleMatrixFactory}
import scala.collection.JavaConverters._

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
    val sc = new SparkContext(config)

    val sparkExecutor = new SparkExecutor(sc)

    def stop(){
      sc.stop
    }

    def execute(program: Executable, config: RuntimeConfiguration): Double = {
      val finalProgram = terminateExecutable(program)

      val pattern = """Job finished: [^,]*, took ([0-9\.]*) s""".r
      val sparkLogger = Logger.getLogger("org.apache.spark.SparkContext")
      sparkLogger.setLevel(Level.INFO)
      val gilbertTimer = new GilbertTimer(pattern)
      sparkLogger.addAppender(new WriterAppender(new SimpleLayout(), gilbertTimer))

      config.checkpointDir foreach { dir => sparkExecutor.sc.setCheckpointDir(dir) }

      sparkExecutor.run(finalProgram, config)

      gilbertTimer.totalTime
    }
  }

  def local(engineConfiguration: EngineConfiguration): ExecutionEngine = {
    val master = "local[" + engineConfiguration.parallelism + "]"

    createSparkExecutionEngine(master, engineConfiguration)
  }

  def remote(engineConfiguration: EngineConfiguration): ExecutionEngine = {
    val master = "spark://" + engineConfiguration.master +":" + engineConfiguration.port

    createSparkExecutionEngine(master, engineConfiguration)
  }

  def createSparkExecutionEngine(master: String, engineConfiguration: EngineConfiguration): ExecutionEngine = {
    val sparkConf = new SparkConf().
      setMaster(master).
      setAppName(engineConfiguration.appName).
      setJars(engineConfiguration.jars).
      set("spark.cores.max", engineConfiguration.parallelism.toString).
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    new SparkExecutionEngine(sparkConf)
  }
}

object withStratosphere{
  class StratosphereExecutionEngine(val executor: PlanExecutor, val appName: String, val parallelism: Int) extends
  ExecutionEngine {
    val translator = new StratosphereExecutor()

    def stop() {}

    def execute(program: Executable, configuration: RuntimeConfiguration): Double = {
      val finalProgram = terminateExecutable(program)

      val plan = translator.run(finalProgram, configuration) match {
        case x:ScalaPlan => x
        case x:ScalaSink[_] => new ScalaPlan(Seq(x))
        case x:List[_] =>
          val sinks = for(dataset <- x) yield dataset.asInstanceOf[ScalaSink[_]]
          new ScalaPlan(sinks)
      }

      plan.setDefaultParallelism(parallelism)
      plan.setJobName(appName)

      val result = executor.executePlan(plan)

      result.getNetRuntime
    }
  }

  def local(engineConfiguration: EngineConfiguration): ExecutionEngine = {
    val executor = new LocalExecutor
    executor.setDefaultOverwriteFiles(true)

    new StratosphereExecutionEngine(executor, engineConfiguration.appName, engineConfiguration.parallelism)
  }

  def remote(engineConfiguration: EngineConfiguration): ExecutionEngine = {

    val executor = new RemoteExecutor(engineConfiguration.master, engineConfiguration.port,
      engineConfiguration.jars.asJava)

    new StratosphereExecutionEngine(executor, engineConfiguration.appName, engineConfiguration.parallelism)
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
