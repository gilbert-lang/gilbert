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

package org.gilbertlang.runtime.execution.spark

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.gilbertlang.runtime._
import org.apache.spark.serializer.KryoSerializer
import org.gilbertlang.runtime.Executables._
import org.gilbertlang.runtimeMacros.linalg.{Configuration, SquareBlockPartitionPlan, SubmatrixBoolean, Submatrix}
import org.gilbertlang.runtime.execution.stratosphere.CellEntry
import org.apache.spark.SparkContext
import org.gilbertlang.runtime.RuntimeTypes.{BooleanType, MatrixType, DoubleType}


class SparkExecutor extends Executor {

  type Matrix = RDD[Submatrix]
  type BooleanMatrix = RDD[SubmatrixBoolean]
  type CellArray = RDD[CellEntry]

  System.setProperty("spark.serializer", classOf[KryoSerializer].getName)
  System.setProperty("spark.kryo.registrator", classOf[MahoutKryoRegistrator].getName)

  val numWorkerThreads = 2
  val degreeOfParallelism = 2*numWorkerThreads
  val sc = new SparkContext("local["+numWorkerThreads+"]", "Gilbert")

  var iterationState: Matrix = null

  protected def execute(executable: Executable): Any = {

    executable match {
      case compound: CompoundExecutable =>
        handle[CompoundExecutable, Unit](
        compound,
        {_ => ()},
        { (compoundExec, _) => compoundExec.executables foreach { evaluate[Any]}}
        )

      case loadMatrix: LoadMatrix =>
        handle[LoadMatrix, (String, Int, Int)](
        loadMatrix,
        {input => (evaluate[String](input.path), evaluate[Double](input.numRows).toInt,
          evaluate[Double](input.numColumns).toInt)},
        { case (_, (path, rows, cols)) =>
          val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows, cols)
          val bcPartitionPlan = sc.broadcast(partitionPlan)

          val entries = sc.textFile(path) map { line =>
            val splits = line.split(" ")
            val row = splits(0).toInt-1
            val col = splits(1).toInt-1
            val value = splits(2).toDouble
            val partitionId = bcPartitionPlan.value.partitionId(row,col)
            (partitionId, (row, col, value))
          }

          val blockSeq = for(partition <- partitionPlan.toSeq) yield (partition.id, partition)
          val blocks = sc.parallelize(blockSeq)

          val submatrices = blocks.cogroup(entries).map{ case (partitionId, (blocks, entries)) =>
            require(blocks.length==1)

            val partition = blocks(0)
            Submatrix(partition, entries)
          }

          submatrices
        }
        )

      case scalarValue: scalar =>
        handle[scalar, Unit](
        scalarValue,
        {_ => ()},
        {(scalarValue, _) => scalarValue.value}
        )

      case stringValue: string =>
        handle[string, Unit](
        stringValue,
        {_ => ()},
        {(stringValue, _) => stringValue.value}
        )

      case writeMatrix: WriteMatrix =>
        writeMatrix.matrix.getType match {
          case MatrixType(DoubleType, _, _) =>
            handle[WriteMatrix, Matrix](
            writeMatrix,
            {input => evaluate[Matrix](input.matrix)},
            {(_, matrixRDD) =>
              matrixRDD foreach { matrix => println(matrix)}
            }
            )
          case MatrixType(BooleanType, _, _) =>
            handle[WriteMatrix, BooleanMatrix](
            writeMatrix,
            {input => evaluate[BooleanMatrix](input.matrix)},
            {(_, matrixRDD) =>
              matrixRDD foreach { matrix => println(matrix)}
            }
            )
          case tpe => throw new SparkExecutionError(s"Cannot write matrix of type $tpe.")
        }

    }
  }

}
