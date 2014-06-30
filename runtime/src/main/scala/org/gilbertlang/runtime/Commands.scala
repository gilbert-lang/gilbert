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

import org.gilbertlang.runtime.execution.reference.ReferenceExecutor
import org.gilbertlang.runtime.execution.spark.SparkExecutor
import org.gilbertlang.runtime.execution.stratosphere.StratosphereExecutor
import eu.stratosphere.api.scala.ScalaPlan
import eu.stratosphere.api.scala.ScalaSink
import Executables._
import eu.stratosphere.client.{RemoteExecutor, LocalExecutor}
import org.gilbertlang.runtimeMacros.linalg.MatrixFactory
import org.gilbertlang.runtimeMacros.linalg.breeze.{BreezeBooleanMatrixFactory, BreezeDoubleMatrixFactory}
import org.gilbertlang.runtimeMacros.linalg.mahout.{MahoutBooleanMatrixFactory, MahoutDoubleMatrixFactory}
import scala.collection.JavaConverters._

object local {
  def apply(executable: Executable) = {

    val write = executable match {
      case matrix: Matrix => WriteMatrix(matrix)
      case scalar: ScalarRef => WriteScalar(scalar)
      case string: StringRef => WriteString(string)
      case function: FunctionRef => WriteFunction(function)
      case _ => executable
    }

    new ReferenceExecutor().run(write)
  }
}

object withSpark {
  class ExecutorWrapper(executable: Executable){
    def local(numWorkerThreads: Int = 4, checkpointDir: String = "", iterationsUntilCheckpoint: Int = 0,
              appName: String = "Gilbert", outputPath: Option[String] = None) {
      val master = "local[" + numWorkerThreads + "]";
      val sparkExecutor = new SparkExecutor(master, checkpointDir, iterationsUntilCheckpoint,appName, numWorkerThreads, outputPath);

      sparkExecutor.run(executable);

      sparkExecutor.stop()
    }

    def remote(master: String, checkpointDir: String = "", iterationsUntilCheckpoint: Int = 0,
               appName: String = "Gilbert",parallelism: Int,
               outputPath: Option[String] = None, jars: Seq[String] = Seq[String]()) {
      val sparkExecutor = new SparkExecutor(master, checkpointDir, iterationsUntilCheckpoint, appName, parallelism,outputPath,
        jars);

      sparkExecutor.run(executable);

      sparkExecutor.stop();
    }
  }

  def apply(executable: Executable) = {

    val writeExecutable = executable match {
      case matrix: Matrix => WriteMatrix(matrix)
      case scalar: ScalarRef => WriteScalar(scalar)
      case string: StringRef => WriteString(string)
      case function: FunctionRef => WriteFunction(function)
      case _ => executable
    }

    new ExecutorWrapper(writeExecutable);
  }
}

object withStratosphere{
  class ExecutorWrapper(executable: Executable){
    def compile(outputPath: String, degreeOfParallelism: Int) = {
      val executor = new StratosphereExecutor(outputPath)

      val result = executor.execute(executable)

      val plan = result match {
        case x:ScalaPlan => x
        case x:ScalaSink[_] => new ScalaPlan(Seq(x))
        case x:List[_] =>
          val sinks = for(dataset <- x) yield dataset.asInstanceOf[ScalaSink[_]]
          new ScalaPlan(sinks)
      }

      plan.setDefaultParallelism(degreeOfParallelism)
      plan
    }
    def local(degreeOfParallelism: Int, outputPath: Option[String] = None){
      val path = outputPath.getOrElse("file://" + System.getProperty("user.dir"))
      val plan = compile(path, degreeOfParallelism)

      LocalExecutor.execute(plan);
    }

    def remote(jobmanager: String, port: Int, degreeOfParallelism: Int, outputPath: String, jars: List[String]){
      val plan = compile(outputPath, degreeOfParallelism)
      val executor = new RemoteExecutor(jobmanager, port, jars.asJava);

      executor.executePlan(plan)
    }
  }

  def apply(executable: Executable) = {
    val writeExecutable = executable match {
      case matrix: Matrix => WriteMatrix(matrix)
      case scalar: ScalarRef => WriteScalar(scalar)
      case string: StringRef => WriteString(string)
      case function: FunctionRef => WriteFunction(function)
      case _ => executable
    }

    new ExecutorWrapper(writeExecutable);
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
