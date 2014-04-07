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
import org.gilbertlang.runtimeMacros.linalg._
import org.apache.spark.SparkContext
import org.gilbertlang.runtime.RuntimeTypes._
import org.gilbertlang.runtime.Operations._
import org.apache.commons.io.FileUtils
import java.io.File
import org.gilbertlang.runtime.execution.UtilityFunctions.binarize
import org.gilbertlang.runtime.shell.PlanPrinter
import breeze.linalg.*
import org.gilbertlang.runtime.execution.UtilityFunctions
import org.gilbertlang.runtime.Executables.VectorwiseMatrixTransformation
import org.gilbertlang.runtime.Executables.WriteMatrix
import org.gilbertlang.runtime.Executables.TypeConversionMatrix
import org.gilbertlang.runtime.Executables.CellArrayReferenceCellArray
import org.gilbertlang.runtime.Executables.WriteFunction
import org.gilbertlang.runtime.Executables.LoadMatrix
import org.gilbertlang.runtime.Executables.CompoundExecutable
import org.gilbertlang.runtime.Executables.UnaryScalarTransformation
import org.gilbertlang.runtime.Executables.scalar
import org.gilbertlang.runtime.Executables.ScalarMatrixTransformation
import org.gilbertlang.runtime.Executables.WriteScalar
import org.gilbertlang.runtime.Executables.TypeConversionScalar
import org.gilbertlang.runtime.Executables.CellwiseMatrixTransformation
import org.gilbertlang.runtime.Executables.MatrixMult
import org.gilbertlang.runtime.Executables.boolean
import org.gilbertlang.runtime.Executables.string
import org.gilbertlang.runtime.Executables.ScalarScalarTransformation
import org.gilbertlang.runtime.Executables.CellArrayExecutable
import org.gilbertlang.runtimeMacros.linalg.SquareBlockPartitionPlan
import org.gilbertlang.runtime.Executables.AggregateMatrixTransformation
import org.gilbertlang.runtime.Executables.WriteCellArray
import org.gilbertlang.runtime.Executables.CellwiseMatrixMatrixTransformation
import org.gilbertlang.runtime.Executables.MatrixScalarTransformation
import org.gilbertlang.runtime.Executables.Transpose
import org.gilbertlang.runtime.RuntimeTypes.MatrixType
import org.gilbertlang.runtime.Executables.WriteString
import scala.language.postfixOps
import org.gilbertlang.runtime.execution.stratosphere.GaussianRandom
import scala.util.control.Breaks.{break, breakable}


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

  private var iterationStateMatrix: Matrix = null
  private var iterationStateCellArray: CellArray = null
  private var convergencePreviousStateMatrix: Matrix = null
  private var convergenceCurrentStateMatrix: Matrix = null
  private var convergencePreviousStateCellArray: CellArray = null
  private var convergenceCurrentStateCellArray: CellArray = null

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

      case booleanValue: boolean =>
        handle[boolean, Unit](
        booleanValue,
        { _ => () },
        { (booleanValue, _) => booleanValue.value}
        )

      case cellArray: CellArrayExecutable =>
        handle[CellArrayExecutable, List[Any]](
        cellArray,
        {input => input.elements map { evaluate[Any]}},
        { (cellArray, elements) =>
          val cellEntries = for((element, index) <- elements.zipWithIndex ) yield {
            val tpe = cellArray.getType.elementTypes(index)
            tpe match {
              case BooleanType | DoubleType | StringType =>
                val entry = CellEntry(index, element)
                sc.parallelize(Seq(entry))
              case MatrixType(BooleanType,_ ,_) => element.asInstanceOf[BooleanMatrix] map { matrix => CellEntry(
                index, matrix) }
              case MatrixType(DoubleType, _, _) => element.asInstanceOf[Matrix] map { matrix => CellEntry(index,
                matrix)}
              case CellArrayType(_) => element.asInstanceOf[CellArray] map { entry => CellEntry(index, entry)}
              case MatrixType(_, _, _) | Void | Undefined | FunctionType | Unknown => throw new SparkExecutionError(
                s"Cannot insert element of type $tpe into cell array.")
            }
          }

          val firstEntry = cellEntries.head
          cellEntries.tail.foldLeft(firstEntry)(_ union _)
        }
        )

      case cellArrayReferenceCellArray: CellArrayReferenceCellArray =>
        handle[CellArrayReferenceCellArray, CellArray](
        cellArrayReferenceCellArray,
        { input => evaluate[CellArray](input.parent)},
        { (ref, cellArrayRDD) =>
         cellArrayRDD filter { entry => entry.idx == ref.reference } map { filteredEntry => filteredEntry.value
           .asInstanceOf[CellEntry] }
        }
        )

      case cellArrayReferenceMatrix: CellArrayReferenceMatrix =>
        handle[CellArrayReferenceMatrix, CellArray](
        cellArrayReferenceMatrix,
        { input => evaluate[CellArray](input.parent)},
        { (ref, cellArrayRDD) =>
          val filtered = cellArrayRDD filter { entry => entry.idx == ref.reference }

          ref.getType match {
            case MatrixType(DoubleType, _, _) => filtered map { filteredEntry => filteredEntry.value.
              asInstanceOf[Matrix]}
            case MatrixType(BooleanType, _, _) => filtered map { filteredEntry => filteredEntry.value.asInstanceOf[
              BooleanMatrix]}
            case tpe@ MatrixType(_, _, _) => throw new SparkExecutionError( s"Cannot extract matrix of type $tpe from" +
              s" cell array.")
          }
        }
        )

      case caRefString: CellArrayReferenceString =>
        handle[CellArrayReferenceString, CellArray](
        caRefString,
        { input => evaluate[CellArray](input.parent)},
        { (ref, cellArrayRDD) =>
          val strings = cellArrayRDD filter { entry => entry.idx == ref.reference } map { filteredEntry =>
            filteredEntry.value.asInstanceOf[String] } collect

          require(strings.length == 1, "Scalar entry of cell array can only contain one element.")
          strings(0)
        }
        )

      case caRefScalar: CellArrayReferenceScalar =>
        handle[CellArrayReferenceScalar, CellArray](
        caRefScalar,
        { input => evaluate[CellArray](input.parent)},
        { (ref, cellArrayRDD) =>
          val filtered = cellArrayRDD filter { entry => entry.idx == ref.reference }
          ref.getType match {
            case DoubleType =>
              val doubles = filtered map { filteredEntry => filteredEntry.value.asInstanceOf[Double]} collect

              require(doubles.length == 1, "Scalar entry of cell array can only contain one element.")
              doubles(0)
            case BooleanType =>
              val booleans = filtered map { filteredEntry => filteredEntry.value.asInstanceOf[Boolean]} collect

              require(booleans.length == 1, " Scalar entry of cell array can only contain one element.")
              booleans(0)
            case ScalarType | Unknown | Void => throw new SparkExecutionError("Scalar cell array reference requires " +
              "concrete " +
              "scalar " +
              "type.")
          }
        }
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

      case writeFunction: WriteFunction =>
        handle[WriteFunction, Unit](
        writeFunction,
        {_ => () },
        { (func, _) =>
          PlanPrinter.print(func, 0)
        }
        )

      case transpose: Transpose =>
        transpose.getType match {
          case MatrixType(DoubleType, _, _) =>
            handle[Transpose, Matrix](
            transpose,
            { input => evaluate[Matrix](input.matrix)},
            { (_, matrixRDD) =>
              matrixRDD map { matrix => matrix.t }
            }
            )
          case MatrixType(BooleanType, _, _) =>
            handle[Transpose, BooleanMatrix](
            transpose,
            { input => evaluate[BooleanMatrix](input.matrix)},
            { (_, matrixRDD) =>
              matrixRDD map { matrix => matrix.t }
            }
            )
        }


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

      case scalarMatrix: ScalarMatrixTransformation =>
        scalarMatrix.operation match {
          case operation: LogicOperation =>
            handle[ScalarMatrixTransformation, (Boolean, BooleanMatrix)](
            scalarMatrix,
            { input => (evaluate[Boolean](input.scalar), evaluate[BooleanMatrix](input.matrix))},
            { case (_, (scalar, matrixRDD)) =>
              val bcScalar = sc.broadcast(scalar)
              operation match {
                case And | SCAnd => matrixRDD map { matrix => matrix :& bcScalar.value }
                case Or | SCOr => matrixRDD map { matrix => matrix :| bcScalar.value }
              }
            }
            )
          case operation: ArithmeticOperation =>
            handle[ScalarMatrixTransformation, (Double, Matrix)](
            scalarMatrix,
            { input => (evaluate[Double](input.scalar), evaluate[Matrix](input.matrix))},
            { case (_, (scalar, matrixRDD)) =>
              val bcScalar = sc.broadcast(scalar)
              operation match {
                case Addition => matrixRDD map { matrix => matrix + bcScalar.value }
                case Subtraction => matrixRDD map { matrix => matrix + -bcScalar.value }
                case Multiplication => matrixRDD map { matrix => matrix * bcScalar.value }
                case Division => matrixRDD map { matrix =>
                  val partition = matrix.getPartition
                  val result = Submatrix.init(partition, bcScalar.value)
                  result / matrix
                }
                case Exponentiation =>
                  matrixRDD map { matrix =>
                    val partition = matrix.getPartition
                    val result = Submatrix.init(partition, bcScalar.value)
                    result :^ matrix
                  }
              }
            }
            )
          case operation: ComparisonOperation =>
            handle[ScalarMatrixTransformation, (Double, Matrix)](
            scalarMatrix,
            { input => (evaluate[Double](input.scalar), evaluate[Matrix](input.matrix))},
            { case (_, (scalar, matrixRDD)) =>
              val bcScalar = sc.broadcast(scalar)
              operation match {
                case GreaterThan => matrixRDD map { matrix => matrix :< bcScalar.value }
                case GreaterEqualThan => matrixRDD map { matrix => matrix :<= bcScalar.value }
                case LessThan => matrixRDD map { matrix => matrix :> bcScalar.value }
                case LessEqualThan => matrixRDD map { matrix => matrix :>= bcScalar.value }
                case Equals => matrixRDD map { matrix => matrix :== bcScalar.value }
                case NotEquals => matrixRDD map { matrix => matrix :!= bcScalar.value }
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

      case matrixMult: MatrixMult =>
        handle[MatrixMult, (Matrix, Matrix)](
        matrixMult,
        {input => (evaluate[Matrix](input.left), evaluate[Matrix](input.right))},
        { case (_, (leftRDD, rightRDD)) =>
          val leftColIDRDD = leftRDD map { matrix => (matrix.columnIndex, matrix)}
          val rightRowIDRDD = rightRDD map { matrix => (matrix.rowIndex, matrix)}

          val localMatrixMults = leftColIDRDD.join(rightRowIDRDD) map { case (_, (leftMatrix,
        rightMatrix)) => ((leftMatrix.rowIndex,
            rightMatrix.columnIndex), leftMatrix*rightMatrix) }

          localMatrixMults reduceByKey( _ + _ ) map { case (key, value) => value }
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

      case matrixScalar: MatrixScalarTransformation =>
        matrixScalar.operation match {
          case operation: LogicOperation =>
            handle[MatrixScalarTransformation, (BooleanMatrix, Boolean)](
            matrixScalar,
            { input => (evaluate[BooleanMatrix](input.matrix), evaluate[Boolean](input.scalar))},
            { case (_, (matrixRDD, scalar)) =>
              val bcScalar = sc.broadcast(scalar)
              operation match {
                case And | SCAnd => matrixRDD map { matrix => matrix :& bcScalar.value }
                case Or | SCOr => matrixRDD map { matrix => matrix :| bcScalar.value }
              }
            }
            )
          case operation: ComparisonOperation =>
            handle[MatrixScalarTransformation, (Matrix, Double)](
            matrixScalar,
            {input => (evaluate[Matrix](input.matrix), evaluate[Double](input.scalar))},
            { case (_, (matrixRDD, scalar)) =>
              val bcScalar = sc.broadcast(scalar)
              operation match {
                case GreaterThan => matrixRDD map { matrix => matrix :> bcScalar.value }
                case GreaterEqualThan => matrixRDD map { matrix => matrix :>= bcScalar.value }
                case LessThan => matrixRDD map { matrix => matrix :< bcScalar.value }
                case LessEqualThan => matrixRDD map { matrix => matrix :<= bcScalar.value }
                case Equals => matrixRDD map { matrix => matrix :== bcScalar.value }
                case NotEquals => matrixRDD map { matrix => matrix :!= bcScalar.value }
              }
            }
            )
          case operation: ArithmeticOperation =>
            handle[MatrixScalarTransformation, (Matrix, Double)](
            matrixScalar,
            { input => (evaluate[Matrix](input.matrix), evaluate[Double](input.scalar))},
            { case (_, (matrixRDD, scalar)) =>
              val bcScalar = sc.broadcast(scalar)
              operation match {
                case Addition => matrixRDD map { matrix => matrix + bcScalar.value }
                case Subtraction => matrixRDD map { matrix => matrix - bcScalar.value }
                case Multiplication => matrixRDD map { matrix => matrix * bcScalar.value }
                case Division => matrixRDD map { matrix => matrix / bcScalar.value }
                case Exponentiation => matrixRDD map { matrix => matrix :^ bcScalar.value }
              }
            }
            )
        }

      case vectorwiseMatrixTransformation: VectorwiseMatrixTransformation =>
        handle[VectorwiseMatrixTransformation, Matrix](
        vectorwiseMatrixTransformation,
        { input => evaluate[Matrix](input.matrix)},
        { (vectorwise, matrixRDD) =>
          vectorwise.operation match {
            case Maximum =>
              val blockwiseMax = matrixRDD map { matrix => (matrix.rowIndex, breeze.linalg.max( matrix(*, ::) ))}
              val totalMax = blockwiseMax.reduceByKey( numerics.max(_, _))
              totalMax map { case (_, subvector) => subvector.asMatrix }
            case Minimum =>
              val blockwiseMax = matrixRDD map { matrix => (matrix.rowIndex, breeze.linalg.min( matrix(*, ::) ))}
              val totalMax = blockwiseMax.reduceByKey( numerics.min(_, _))
              totalMax map { case (_, subvector) => subvector.asMatrix }
            case Norm2 =>
              val squaredMatrix = matrixRDD map { matrix => matrix :^ 2.0 }
              val blockwiseSumSquared = squaredMatrix map { matrix => (matrix.rowIndex, breeze.linalg.sum(matrix(*,
              ::)))}
              val totalSumSquared = blockwiseSumSquared.reduceByKey(_ + _)
              totalSumSquared map { case (_, subvector) =>
                val matrix = subvector.asMatrix
                matrix mapActiveValues { math.sqrt }
              }
            case NormalizeL1 =>
              val blockwiseNorm = matrixRDD map { matrix => (matrix.rowIndex, breeze.linalg.norm(matrix(*, ::), 1))}
              val l1Norm = blockwiseNorm reduceByKey ( _ + _ )

              val keyedMatrix = matrixRDD map { matrix => (matrix.rowIndex, matrix)}
              keyedMatrix.join(l1Norm).map{case (_, (matrix, l1norm)) =>
                for(col <- 0 until matrix.cols){
                  matrix(::, col) :/= l1norm
                }
              }
          }
        }
        )

      case cellwiseMM: CellwiseMatrixMatrixTransformation =>
        cellwiseMM.operation match {
          case operation: LogicOperation =>
            handle[CellwiseMatrixMatrixTransformation, (BooleanMatrix, BooleanMatrix)](
            cellwiseMM,
            { input => (evaluate[BooleanMatrix](input.left), evaluate[BooleanMatrix](input.right))},
            { case (_, (leftMatrixRDD, rightMatrixRDD)) =>
              val keyedLeft = leftMatrixRDD map { matrix => ((matrix.rowIndex, matrix.columnIndex), matrix)}
              val keyedRight= rightMatrixRDD map { matrix => ((matrix.rowIndex, matrix.columnIndex), matrix)}

              operation match {
                case SCAnd | And => keyedLeft join keyedRight map { case (_, (left, right)) => left :& right }
                case SCOr | Or => keyedLeft join keyedRight map { case (_, (left, right)) => left :| right }
              }
            }
            )
          case operation: ArithmeticOperation =>
            handle[CellwiseMatrixMatrixTransformation, (Matrix, Matrix)](
            cellwiseMM,
            { input => (evaluate[Matrix](input.left), evaluate[Matrix](input.right))},
            { case (_, (leftMatrixRDD, rightMatrixRDD)) =>
              val keyedLeft = leftMatrixRDD map { matrix => ((matrix.rowIndex, matrix.columnIndex), matrix) }
              val keyedRight = rightMatrixRDD map { matrix => ((matrix.rowIndex, matrix.columnIndex), matrix) }

              operation match {
                case Addition => keyedLeft join keyedRight map { case (_, (left, right)) => left + right }
                case Subtraction => keyedLeft join keyedRight map { case (_, (left, right)) => left - right }
                case Multiplication => keyedLeft join keyedRight map { case (_, (left, right)) => left :* right }
                case Division => keyedLeft join keyedRight map { case (_, (left, right)) => left :/ right }
                case Exponentiation => keyedLeft join keyedRight map { case (_, (left, right)) => left :^ right }
              }
            }
            )
          case operation: ComparisonOperation =>
            handle[CellwiseMatrixMatrixTransformation, (Matrix, Matrix)](
            cellwiseMM,
            { input => (evaluate[Matrix](input.left), evaluate[Matrix](input.right))},
            { case (_, (leftMatrixRDD, rightMatrixRDD)) =>
              val keyedLeft = leftMatrixRDD map { matrix => ((matrix.rowIndex, matrix.columnIndex), matrix)}
              val keyedRight = rightMatrixRDD map { matrix => ((matrix.rowIndex, matrix.columnIndex), matrix)}

              operation match {
                case GreaterThan => keyedLeft join keyedRight map { case (_, (left, right)) => left :> right }
                case GreaterEqualThan => keyedLeft join keyedRight map { case (_, (left, right)) => left :>= right }
                case LessThan => keyedLeft join keyedRight map { case (_, (left, right)) => left :< right }
                case LessEqualThan => keyedLeft join keyedRight map { case (_, (left, right)) => left :<= right }
                case Equals => keyedLeft join keyedRight map { case (_, (left, right)) => left :== right }
                case NotEquals => keyedLeft join keyedRight map { case (_, (left ,right)) => left :!= right }
              }
            }
            )
          case operation: MinMax =>
            handle[CellwiseMatrixMatrixTransformation, (Matrix, Matrix)](
            cellwiseMM,
            { input => (evaluate[Matrix](input.left), evaluate[Matrix](input.right))},
            { case (_, (leftMatrixRDD, rightMatrixRDD)) =>
              val keyedLeft = leftMatrixRDD map { matrix => ((matrix.rowIndex, matrix.columnIndex), matrix)}
              val keyedRight = rightMatrixRDD map { matrix => ((matrix.rowIndex, matrix.columnIndex), matrix)}

              operation match {
                case Minimum => keyedLeft join keyedRight map { case (_, (left, right)) => numerics.min(left, right)}
                case Maximum => keyedLeft join keyedRight map { case (_, (left, right)) => numerics.max(left, right)}
              }
            }
            )
        }

      case cellwiseM: CellwiseMatrixTransformation =>
        handle[CellwiseMatrixTransformation, Matrix](
        cellwiseM,
        { input => evaluate[Matrix](input.matrix)},
        { (cellwiseM, matrixRDD) =>
          cellwiseM.operation match {
            case Binarize => matrixRDD map { matrix => matrix mapActiveValues {UtilityFunctions.binarize} }
            case Minus => matrixRDD map { matrix => matrix * -1.0 }
            case Abs => matrixRDD map { matrix => matrix.mapActiveValues{ value => math.abs(value) }}
          }
        }
        )

      case typeConversion: TypeConversionMatrix =>
        (typeConversion.sourceType, typeConversion.targetType) match {
          case (MatrixType(BooleanType, _, _), MatrixType(DoubleType, _, _)) =>
            handle[TypeConversionMatrix, Matrix](
            typeConversion,
            { input => evaluate[Matrix](input.matrix)},
            { (_, matrixRDD) =>
              matrixRDD map { matrix =>
                matrix map ( value => if(value) 1.0 else 0.0)
              }
            }
            )
          case (srcType, targetType) => throw new SparkExecutionError(s"Cannot convert matrix from type $srcType to" +
            s" type $targetType.")
        }

      case typeConversion: TypeConversionScalar =>
        (typeConversion.sourceType, typeConversion.targetType) match {
          case (BooleanType, DoubleType) =>
            handle[TypeConversionScalar, Boolean](
            typeConversion,
            { input => evaluate[Boolean](input.scalar)},
            { (_, scalarValue) => if(scalarValue) 1.0 else 0.0 }
            )
          case (srcType, targetType) => throw new SparkExecutionError(s"Cannot convert scalar from type $srcType to " +
            s"type $targetType.")
        }

      case sparseOnes: spones =>
        handle[spones, Matrix](
        sparseOnes,
        {input => evaluate[Matrix](input.matrix)},
        {(_, matrixRDD) =>
          matrixRDD map {matrix => matrix mapActiveValues( binarize )}
        }
        )

      case z: zeros =>
        handle[zeros, (Int, Int)](
        z,
        { input => (evaluate[Double](input.numRows).toInt, evaluate[Double](input.numCols).toInt)},
        { case (_, (rows, cols)) =>
          val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows, cols)
          sc.parallelize(partitionPlan.toSeq) map { partition => Submatrix(partition)}
        }
        )

      case o: ones =>
        handle[ones, (Int, Int)](
        o,
        { input => (evaluate[Double](input.numRows).toInt, evaluate[Double](input.numColumns).toInt)},
        { case (_, (rows, cols)) =>
          val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows, cols)
          sc.parallelize(partitionPlan.toSeq) map { partition => Submatrix.init(partition, 1.0)}
        }
        )

      case e: eye =>
        handle[eye, (Int, Int)](
        e,
        { input => (evaluate[Double](input.numRows).toInt, evaluate[Double](input.numCols).toInt)},
        { case (_, (rows, cols)) =>
          val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows, cols)

          sc.parallelize(partitionPlan.toSeq) map { partition => Submatrix.eye(partition) }
        }
        )

      case r: randn =>
        handle[randn, (Int, Int, Double, Double)](
        r,
        { input => (evaluate[Double](input.numRows).toInt, evaluate[Double](input.numColumns).toInt,
          evaluate[Double](input.mean), evaluate[Double](input.std))},
        { case (_,(rows, cols, mean, std)) =>
          val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows, cols)
          val bcMean = sc.broadcast(mean)
          val bcStd = sc.broadcast(std)
          sc.parallelize(partitionPlan.toSeq) map { partition =>
            val randomGenerator = new GaussianRandom(bcMean.value, bcStd.value)
            Submatrix.rand(partition, randomGenerator)
          }
        }
        )

      case s: sum =>
        handle[sum, (Matrix, Int)](
        s,
        { input => (evaluate[Matrix](input.matrix), evaluate[Double](input.dimension).toInt)},
        { case (_, (matrixRDD, dim)) =>
          dim match {
            case 1 =>
              val blockwiseSum = matrixRDD map { matrix => (matrix.columnIndex, breeze.linalg.sum(matrix(::, *)))}
              blockwiseSum reduceByKey { _ + _ } map { case (_, sumMatrix) => sumMatrix }
            case 2 =>
              val blockwiseSum = matrixRDD map { matrix => (matrix.rowIndex, breeze.linalg.sum(matrix(*, ::)))}
              blockwiseSum reduceByKey { _ + _ } map { case (_, sumVector) => sumVector.asMatrix }
            case _ => throw new SparkExecutionError(s"Sum does not support dimension $dim.")
          }
        }
        )

      case sRow: sumRow =>
        handle[sumRow, Matrix](
        sRow,
        { input => evaluate[Matrix](input.matrix)},
        { (_, matrixRDD) =>
          val blockwiseSum = matrixRDD map { matrix => (matrix.rowIndex, breeze.linalg.sum(matrix(*, ::)))}
          blockwiseSum reduceByKey { _ + _ } map { case (_, sumVector) => sumVector.asMatrix }
        }
        )

      case sCol: sumCol =>
        handle[sumCol, Matrix](
        sCol,
        { input => evaluate[Matrix](input.matrix)},
        { (_, matrixRDD) =>
          val blockwiseSum = matrixRDD map { matrix => (matrix.columnIndex, breeze.linalg.sum(matrix(::, *)))}
          blockwiseSum reduceByKey { _ + _ } map { case (_, sumMatrix) => sumMatrix }
        }
        )

      case d: diag =>
        handle[diag, Matrix](
        d,
        { input => evaluate[Matrix](input.matrix)},
        { case (d, matrixRDD) =>
          (d.matrix.rows, d.matrix.cols) match {
            case (Some(1), _) =>
              val constructionInfo = matrixRDD map { matrix => (matrix.columnIndex, matrix.cols, matrix.columnOffset)}
              constructionInfo cartesian matrixRDD map {
                case ((rowIdx, rows, rowOffset), submatrix) =>
                val partition = Partition(-1, rowIdx, submatrix.columnIndex, rows, submatrix.cols, rowOffset,
                  submatrix.columnOffset, submatrix.totalColumns, submatrix.totalColumns)

                if(rowIdx == submatrix.columnIndex){
                  val result = Submatrix(partition, submatrix.cols)

                  for(idx <- 0 until submatrix.cols){
                    result.update(idx,idx, submatrix(0, idx))
                  }

                  result
                }else{
                  Submatrix(partition)
                }
              }
            case (_, Some(1)) =>
              val constructionInfo = matrixRDD map { matrix => (matrix.rowIndex, matrix.rows, matrix.rowOffset)}
              constructionInfo cartesian matrixRDD map {
                case ((colIdx, cols, colOffset), submatrix) =>
                val partition = Partition(-1, submatrix.rowIndex, colIdx, submatrix.rows, cols, submatrix.rowOffset,
                  colOffset, submatrix.totalRows, submatrix.totalRows)

                if(colIdx == submatrix.rowIndex){
                  val result = Submatrix(partition, submatrix.rows)

                  for(idx <- 0 until submatrix.rows){
                    result.update(idx, idx, submatrix(idx, 0))
                  }

                  result
                }else{
                  Submatrix(partition)
                }
              }
            case _ =>
              val blockwiseDiagonal = matrixRDD map { matrix =>
                val partition = Partition(-1, matrix.rowIndex, 0, matrix.rows, 1, matrix.rowOffset, 0,
                  matrix.totalRows, 1)

                val result = Submatrix(partition, matrix.rows)

                Submatrix.containsDiagonal(matrix.getPartition) match {
                  case None => result
                  case Some((rowStart, colStart)) =>
                    for(idx <- 0 until math.min(matrix.rows - rowStart, matrix.cols - colStart)) yield {
                      result.update(idx+rowStart, 0, matrix(idx+rowStart, idx+colStart))
                    }
                    result
                }
              }

              blockwiseDiagonal map { matrix => (matrix.rowIndex, matrix)} reduceByKey { _ + _ } map { case (_,
              matrix) => matrix}
          }
        }
        )

      case linearSpace: linspace =>
        handle[linspace, (Double, Double, Int)](
        linearSpace,
        { input => (evaluate[Double](input.start), evaluate[Double](input.end), evaluate[Double](input.numPoints)
          .toInt)},
        { case (_, (start, end, numPoints)) =>
          val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, 1, numPoints)
          val bcStepWidth = sc.broadcast((end-start)/(numPoints-1))

          sc.parallelize(partitionPlan.toSeq) map { partition =>
            val result = Submatrix(partition, partition.numColumns)

            for(counter <- partition.columnOffset until (partition.columnOffset + partition.numColumns)){
              result.update(0, counter - partition.columnOffset, counter * bcStepWidth.value)
            }

            result
          }
        }
        )

      case pairDistance: pdist2 =>
        handle[pdist2, (Matrix, Matrix)](
        pairDistance,
        { input => (evaluate[Matrix](input.matrixA), evaluate[Matrix](input.matrixB))},
        { case (_, (matrixARDD, matrixBRDD)) =>
          val keyedMA = matrixARDD map { matrix => (matrix.columnIndex, matrix)}
          val keyedMB = matrixBRDD map { matrix => (matrix.columnIndex, matrix)}

          val blockwiseDist = keyedMA join keyedMB map { case (_, (matrixA, matrixB)) =>
            val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, matrixA.totalRows,
              matrixB.totalRows)

            val entries = for(rowA <- 0 until matrixA.rows; rowB <- 0 until matrixB.rows) yield {
              val diff = matrixA(rowA, ::) - matrixB(rowB, ::)
              val squared = diff :^ 2.0
              val squaredSum = breeze.linalg.sum(squared)
              (rowA + matrixA.rowOffset, rowB + matrixB.rowOffset, squaredSum)
            }

            ((matrixA.rowIndex, matrixB.rowIndex),Submatrix(partitionPlan.getPartition(matrixA.rowIndex,
              matrixB.rowIndex), entries))
          }

          blockwiseDist reduceByKey(_ + _) map { case (_, matrix) => matrix :^ 0.5 }
        }
        )

      case repeatMatrix: repmat =>
        handle[repmat, (Matrix, Int, Int)](
        repeatMatrix,
        {input => (evaluate[Matrix](input.matrix), evaluate[Double](input.numRows).toInt,
          evaluate[Double](input.numCols).toInt)},
        { case (_, (matrixRDD, multRows, multCols)) =>
          val bcMultRows = sc.broadcast(multRows)
          val bcMultCols = sc.broadcast(multCols)

          val newBlocks = matrixRDD flatMap { matrix =>
            val newPartitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE,
              matrix.totalRows*bcMultRows.value, matrix.totalColumns*bcMultCols.value)
            val oldPartitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, matrix.totalRows,
              matrix.totalColumns)

            for(rowIdx <- matrix.rowIndex until matrix.rowIndex * bcMultRows.value by oldPartitionPlan.maxRowIndex;
                colIdx <- matrix.columnIndex until matrix.columnIndex*bcMultCols.value by oldPartitionPlan.
                  maxColumnIndex) yield {
              val partition = newPartitionPlan.getPartition(rowIdx, colIdx)
              (partition.id, partition)
            }
          }

          val newEntries = matrixRDD flatMap { matrix =>
            val newPartitionPlan = SquareBlockPartitionPlan(Configuration.BLOCKSIZE,
              matrix.totalRows* bcMultRows.value, matrix.totalColumns* bcMultCols.value)

            matrix.activeIterator flatMap { case ((row, col), value) =>
              for(rowMult <- 0 until bcMultRows.value; colMult <- 0 until bcMultCols.value) yield {
                val rowIndex = row + rowMult * matrix.totalRows
                val columnIndex = col + colMult * matrix.totalColumns
                (newPartitionPlan.partitionId(rowIndex, columnIndex),
                  (rowIndex, columnIndex, value))
              }
            }
          }

          newBlocks cogroup newEntries map { case (id, (blocks, entries)) =>
            require(blocks.length == 1, "There can only be one block for a partition id.")

            val partition = blocks.head
            Submatrix(partition, entries)
          }
        }
        )

      case minWithIdx: minWithIndex =>
        handle[minWithIndex, (Matrix, Int)](
        minWithIdx,
        { input => (evaluate[Matrix](input.matrix), evaluate[Double](input.dimension).toInt)},
        { case (_, (matrixRDD, dimension)) =>
          dimension match {
            case 1 =>
              val (rows,cols) = matrixRDD map { matrix => (1,matrix.totalColumns)} reduce { (a,b) => a }
              val bcRows = sc.broadcast(rows)
              val bcCols = sc.broadcast(cols)

              val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows, cols)
              val newBlocks = sc.parallelize(partitionPlan.toSeq) map { partition => (partition.id,
                partition)}

              val blockwiseMinIdxValues = matrixRDD flatMap { matrix =>
                for(col <- 0 until matrix.cols) yield {
                  val (minRow, minValue) = matrix(::, col).iterator.minBy { case (row, value) => value}
                  (col+matrix.columnOffset,( minRow, minValue) )
                }
              }

              val minIdxValues = blockwiseMinIdxValues.reduceByKey{ (a,b) => if(a._2 < b._2) a else b }

              val partitionedMinIdxValues = minIdxValues map { case (col, (row, value)) =>
                val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, bcRows.value, bcCols.value)
                val partitionId = partitionPlan.partitionId(0, col)
                (partitionId, (row, col, value))
              }

              newBlocks cogroup partitionedMinIdxValues flatMap { case (_, (blocks, minIdxValues)) =>
                require(blocks.length == 1, "There can only be one block for a partition id.")

                val partition = blocks.head
                val (minValueEntries, minIdxEntries) = minIdxValues.unzip { case (minRow, col, value) =>
                  ((0, col, value), (0, col, minRow.toDouble))
                }

                val minValues = Submatrix(partition, minValueEntries)
                val minIdxs = Submatrix(partition, minIdxEntries)

                Seq(CellEntry(0, minValues), CellEntry(1, minIdxs))
              }
            case 2 =>
              val (rows, cols) = matrixRDD map { matrix => (matrix.totalRows, 1) } reduce { (a,b) => a}
              val bcRows = sc.broadcast(rows)
              val bcCols = sc.broadcast(cols)

              val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows, cols)
              val newBlocks = sc.parallelize(partitionPlan.toSeq) map { partition => (partition.id, partition)}

              val blockwiseMinIdxValues = matrixRDD flatMap { matrix =>
                for(row <- 0 until matrix.rows) yield {
                  val ((minRow, minCol), minValue) = matrix(row, ::).iterator minBy { case ((row, col),
                  value) => value }
                  (minRow + matrix.rowOffset,( minCol, minValue))
                }
              }

              val minIdxValues = blockwiseMinIdxValues.reduceByKey{(a,b) => if(a._2 < b._2) a else b}

              val partitionedMinIdxValues = minIdxValues map { case (row, (minCol, minValue)) =>
                val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, bcRows.value, bcCols.value)
                val partitionId = partitionPlan.partitionId(row, 0)
                (partitionId, (row, minCol, minValue))
              }

              newBlocks cogroup partitionedMinIdxValues flatMap { case (_, (blocks, entries)) =>
                require(blocks.length == 1, "There can only be one block for a partition ID.")

                val partition = blocks.head

                val (minValueEntries, minIdxEntries) = entries unzip { case(row, minCol, minValue) =>
                  ((row, 0, minValue), (row, 0, minCol.toDouble))
                }

                val minValues = Submatrix(partition, minValueEntries)
                val minIdxs = Submatrix(partition, minIdxEntries)

                Seq(CellEntry(0, minValues), CellEntry(1, minIdxs))
              }
            case x => throw new SparkExecutionError(s"Spark executor does not support minWithIndex with dimension $x.")
          }
        }
        )

      case IterationStatePlaceholder => iterationStateMatrix
      case ConvergencePreviousStatePlaceholder => convergencePreviousStateMatrix
      case ConvergenceCurrentStatePlaceholder => convergenceCurrentStateMatrix

      case fixpoint: FixpointIterationMatrix =>
        handle[FixpointIterationMatrix, (Matrix, Int)](
        fixpoint,
        { input => (evaluate[Matrix](input.initialState), evaluate[Double](input.maxIterations).toInt)},
        { case (fix, (initialState, maxIterations)) =>
          iterationStateMatrix = initialState

          breakable{
            for(iteration <- 0 until maxIterations){
              if(fix.convergencePlan != null){
                convergencePreviousStateMatrix = iterationStateMatrix
              }

              iterationStateMatrix = evaluate[Matrix](fix.updatePlan)

              if(fix.convergencePlan != null){
                convergenceCurrentStateMatrix = iterationStateMatrix
                val converged = evaluate[Boolean](fix.convergencePlan)

                if(converged){
                  break
                }
              }
            }
          }

          iterationStateMatrix
        }
        )

      case placeholder: IterationStatePlaceholderCellArray => iterationStateCellArray
      case placeholder: ConvergencePreviousStateCellArrayPlaceholder => convergencePreviousStateCellArray
      case placeholder: ConvergenceCurrentStateCellArrayPlaceholder => convergenceCurrentStateCellArray

      case fixpoint: FixpointIterationCellArray =>
        handle[FixpointIterationCellArray, (CellArray, Int)](
        fixpoint,
        { input => (evaluate[CellArray](input.initialState), evaluate[Double](input.maxIterations).toInt)},
        { case (fix, (initialState, maxIterations)) =>
          iterationStateCellArray = initialState

          breakable{
            for(iteration <- 0 until maxIterations){
              if(fix.convergencePlan != null){
               convergencePreviousStateCellArray = iterationStateCellArray
              }

              iterationStateCellArray = evaluate[CellArray](fix.updatePlan)

              if(fix.convergencePlan != null){
                convergenceCurrentStateCellArray = iterationStateCellArray
                val converged = evaluate[Boolean](fix.convergencePlan)

                if(converged){
                  break
                }
              }
            }
          }
        }
        )


      case _ : Parameter => throw new SparkExecutionError("Cannot execute parameters.")
      case _ : FunctionRef => throw new SparkExecutionError("Cannot execute functions.")

    }
  }

}
