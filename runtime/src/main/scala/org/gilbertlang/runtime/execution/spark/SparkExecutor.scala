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
import org.apache.spark.SparkContext
import org.gilbertlang.runtime.RuntimeTypes.{BooleanType, MatrixType, DoubleType}
import org.gilbertlang.runtime.Operations._
import org.apache.commons.io.FileUtils
import java.io.File
import org.gilbertlang.runtime.Executables.WriteCellArray
import org.gilbertlang.runtime.Executables.scalar
import org.gilbertlang.runtime.Executables.string
import org.gilbertlang.runtime.Executables.ScalarScalarTransformation
import org.gilbertlang.runtime.Executables.WriteMatrix
import org.gilbertlang.runtimeMacros.linalg.SquareBlockPartitionPlan
import org.gilbertlang.runtime.RuntimeTypes.MatrixType
import org.gilbertlang.runtime.Executables.WriteScalar
import org.gilbertlang.runtime.Executables.WriteString
import org.gilbertlang.runtime.Executables.LoadMatrix
import org.gilbertlang.runtime.Executables.AggregateMatrixTransformation
import org.gilbertlang.runtime.Executables.CompoundExecutable
import org.gilbertlang.runtime.execution.UtilityFunctions.binarize


class SparkExecutor extends Executor {

  case class CellEntry(idx: Int, value: Any){
    override def toString = value.toString
  }

  type Matrix = RDD[Submatrix]
  type BooleanMatrix = RDD[SubmatrixBoolean]
  type CellArray = RDD[CellEntry]

  System.setProperty("spark.serializer", classOf[KryoSerializer].getName)
  System.setProperty("spark.kryo.registrator", classOf[MahoutKryoRegistrator].getName)

  val WRITE_TO_OUTPUT = true

  private val numWorkerThreads = 2
  private val degreeOfParallelism = 2*numWorkerThreads
  private val sc = new SparkContext("local["+numWorkerThreads+"]", "Gilbert")

  private var tempFileCounter = 0

  private var iterationState: Matrix = null

  def getCWD: String = System.getProperty("user.dir")

  def newTempFileName(): String = {
    tempFileCounter += 1
    "file://" + getCWD + "/gilbert" + tempFileCounter + ".output"
  }

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
              if(WRITE_TO_OUTPUT){
                matrixRDD foreach { matrix => println(matrix)}
              }else{
                val path = newTempFileName()
                matrixRDD.saveAsTextFile(path)
              }

            }
            )
          case MatrixType(BooleanType, _, _) =>
            handle[WriteMatrix, BooleanMatrix](
            writeMatrix,
            {input => evaluate[BooleanMatrix](input.matrix)},
            {(_, matrixRDD) =>
              if(WRITE_TO_OUTPUT){
                matrixRDD foreach { matrix => println(matrix)}
              }else{
                val path = newTempFileName()
                matrixRDD.saveAsTextFile(path)
              }

            }
            )
          case tpe => throw new SparkExecutionError(s"Cannot write matrix of type $tpe.")
        }

      case writeString: WriteString =>
        handle[WriteString, String](
        writeString,
        {input => evaluate[String](writeString.string)},
        {(_, stringValue) =>
          if(WRITE_TO_OUTPUT){
            println(stringValue)
          }else{
            val path = newTempFileName()
            FileUtils.writeStringToFile(new File(path), stringValue)
          }
        }
        )

      case writeScalar: WriteScalar =>
        writeScalar.scalar.getType match {
          case DoubleType =>
            handle[WriteScalar,Double](
            writeScalar,
            {input => evaluate[Double](input.scalar)},
            {(_, scalarValue) =>
              if(WRITE_TO_OUTPUT){
                println(scalarValue)
              }else{
                val path = newTempFileName()
                FileUtils.writeStringToFile(new File(path), scalarValue.toString)
              }
            }
            )
          case BooleanType =>
            handle[WriteScalar, Boolean](
            writeScalar,
            {input => evaluate[Boolean](input.scalar)},
            {(_, booleanValue) =>
              if(WRITE_TO_OUTPUT){
                println(booleanValue)
              }else{
                val path = newTempFileName()
                FileUtils.writeStringToFile(new File(path), booleanValue.toString)
              }
            }
            )
          case tpe => throw new SparkExecutionError(s"Cannot write scalar of type $tpe.")
        }

      case writeCellArray: WriteCellArray =>
        handle[WriteCellArray, CellArray](
        writeCellArray,
        {input => evaluate[CellArray](input.cellArray)},
        {(writeCellArray, cellArrayValueRDD) =>
          val cellArrayType = writeCellArray.cellArray.getType

          for(idx <- 0 until cellArrayType.elementTypes.length){
            val cellEntryRDD = cellArrayValueRDD filter { entry => entry.idx == idx}

            if(WRITE_TO_OUTPUT){
              cellEntryRDD foreach { println }
            }else{
              val path = newTempFileName()
              cellEntryRDD.saveAsTextFile(path)
            }
          }
        }
        )

      case scalarScalar: ScalarScalarTransformation =>
        scalarScalar.operation match {
          case operation: LogicOperation =>
            handle[ScalarScalarTransformation, (Boolean, Boolean)](
            scalarScalar,
            {input => (evaluate[Boolean](input.left), evaluate[Boolean](input.right))},
            { case(_, (leftValue, rightValue)) =>
              operation match {
                case And => leftValue & rightValue
                case SCAnd => leftValue && rightValue
                case Or => leftValue | rightValue
                case SCOr => leftValue || rightValue
              }
            }
            )
          case operation: ComparisonOperation =>
            handle[ScalarScalarTransformation, (Double, Double)](
            scalarScalar,
            {input => (evaluate[Double](input.left), evaluate[Double](input.right))},
            { case (_, (leftValue, rightValue)) =>
              operation match {
                case GreaterThan => leftValue > rightValue
                case GreaterEqualThan => leftValue >= rightValue
                case LessThan => leftValue < rightValue
                case LessEqualThan => leftValue <= rightValue
                case Equals => leftValue == rightValue
                case NotEquals => leftValue != rightValue
              }
            }
            )
          case operation: MinMax =>
            handle[ScalarScalarTransformation, (Double, Double)](
            scalarScalar,
            { input => (evaluate[Double](input.left), evaluate[Double](input.right))},
            { case (_, (leftValue, rightValue)) =>
              operation match {
                case Minimum => math.min(leftValue, rightValue)
                case Maximum => math.max(leftValue, rightValue)
              }
            }
            )
          case operation: ArithmeticOperation =>
            handle[ScalarScalarTransformation, (Double,Double)](
            scalarScalar,
            { input => (evaluate[Double](input.left), evaluate[Double](input.right))},
            { case (_, (leftValue, rightValue)) =>
              operation match {
                case Addition => leftValue + rightValue
                case Subtraction => leftValue - rightValue
                case Division => leftValue/rightValue
                case Multiplication => leftValue * rightValue
                case Exponentiation => math.pow(leftValue, rightValue)
              }
            }
            )
        }

      case unaryScalar: UnaryScalarTransformation =>
        handle[UnaryScalarTransformation, (Double)](
        unaryScalar,
        { input => evaluate[Double](input.scalar)},
        { (unaryScalar, scalarValue) =>
          unaryScalar.operation match {
            case Abs => math.abs(scalarValue)
            case Binarize => binarize(scalarValue)
            case Minus => -scalarValue
          }
        }
        )

      case aggregate: AggregateMatrixTransformation =>
        handle[AggregateMatrixTransformation, Matrix](
        aggregate,
        {input => evaluate[Matrix](input.matrix)},
        {(aggregate, matrixRDD) =>
          aggregate.operation match {
            case SumAll =>
              val result = matrixRDD map { matrix => matrix.sum } reduce { _ + _ }
              result
            case Maximum =>
              val result = matrixRDD map { matrix => matrix.max } reduce { math.max(_,_) }
              result
            case Minimum =>
              val result = matrixRDD map { matrix => matrix.min } reduce { math.min(_,_) }
              result
            case Norm2 =>
              val squaredSum = matrixRDD map { matrix => (matrix :^ 2.0).sum } reduce { _ + _ }
              math.sqrt(squaredSum)
          }
        }
        )

      case _ : Parameter => throw new SparkExecutionError("Cannot execute parameters.")
      case _ : FunctionRef => throw new SparkExecutionError("Cannot execute functions.")

    }
  }

}
