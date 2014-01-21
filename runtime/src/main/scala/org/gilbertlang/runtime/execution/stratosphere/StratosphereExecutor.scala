package org.gilbertlang.runtime.execution.stratosphere

import org.gilbertlang.runtime.Executor
import org.gilbertlang.runtime.Executable
import org.gilbertlang.runtime.LoadMatrix
import eu.stratosphere.api.scala.operators.CsvInputFormat
import eu.stratosphere.api.scala.DataSource
import org.gilbertlang.runtime.WriteMatrix
import eu.stratosphere.api.scala.DataSet
import eu.stratosphere.api.scala.operators.CsvOutputFormat
import org.gilbertlang.runtime.WriteString
import org.gilbertlang.runtime.WriteFunction
import org.gilbertlang.runtime.WriteScalarRef
import org.gilbertlang.runtime.VoidExecutable
import org.gilbertlang.runtime.ScalarMatrixTransformation
import org.gilbertlang.runtime.Addition
import org.gilbertlang.runtime.Subtraction
import org.gilbertlang.runtime.Multiplication
import org.gilbertlang.runtime.Division
import org.gilbertlang.runtime.MatrixScalarTransformation
import org.gilbertlang.runtime.ScalarScalarTransformation
import org.gilbertlang.runtime.AggregateMatrixTransformation
import org.gilbertlang.runtime.Maximum
import org.gilbertlang.runtime.Minimum
import org.gilbertlang.runtime.Norm2
import org.gilbertlang.runtime.UnaryScalarTransformation
import org.gilbertlang.runtime.Minus
import org.gilbertlang.runtime.Binarize
import org.gilbertlang.runtime.execution.CellwiseFunctions
import org.gilbertlang.runtime.scalar
import org.gilbertlang.runtimeMacros.io.LiteralInputFormat
import org.gilbertlang.runtime.string
import org.gilbertlang.runtime.CellwiseMatrixTransformation
import org.gilbertlang.runtime.CellwiseMatrixMatrixTransformation
import org.gilbertlang.runtime.MatrixMult
import org.gilbertlang.runtime.Transpose
import org.gilbertlang.runtime.VectorwiseMatrixTransformation
import org.gilbertlang.runtime.NormalizeL1
import org.gilbertlang.runtime.ones
import org.gilbertlang.runtime.randn
import org.apache.mahout.math.random.Normal
import org.gilbertlang.runtime.spones
import org.gilbertlang.runtime.sumRow
import org.gilbertlang.runtime.sumCol
import org.gilbertlang.runtime.diag
import eu.stratosphere.api.scala.operators.DelimitedOutputFormat
import org.apache.mahout.math.SparseMatrix
import org.apache.mahout.math.function.DoubleFunction
import org.apache.mahout.math.function.Functions
import org.apache.mahout.math.function.DoubleDoubleFunction
import org.gilbertlang.runtime.execution.VectorFunctions
import scala.collection.convert.WrapAsScala
import org.apache.mahout.math.DenseVector
import MatrixValue._
import scala.collection.mutable.ArrayBuilder
import org.gilbertlang.runtime.FixpointIteration
import org.gilbertlang.runtime.IterationStatePlaceholder
import org.gilbertlang.runtime.CompoundExecutable
import eu.stratosphere.api.scala.ScalaPlan
import eu.stratosphere.api.scala.ScalaSink
import org.gilbertlang.runtime.StringParameter
import org.gilbertlang.runtime.ScalarParameter
import org.gilbertlang.runtime.MatrixParameter
import org.gilbertlang.runtime.FunctionParameter
import org.gilbertlang.runtime.function
import org.gilbertlang.runtime.sum

class StratosphereExecutor extends Executor with WrapAsScala {
  type Entry = Submatrix
  type Matrix = DataSet[Entry]
  type Scalar[T] = DataSet[T]
  private var tempFileCounter = 0
  private var iterationStatePlaceholderValue: Option[Matrix] = None

  implicit def scalaFunction2DoubleFunction(func: Double => Double): DoubleFunction = {
    new DoubleFunction {
      def apply(input: Double) = func(input)
    }
  }

  implicit def javaFunction2DoubleFunction(func: Double => java.lang.Double): DoubleFunction = {
    new DoubleFunction {
      def apply(input: Double) = func(input)
    }
  }

  implicit def scalaFunction2DoubleDoubleFunction(func: (Double, Double) => Double): DoubleDoubleFunction = {
    new DoubleDoubleFunction {
      def apply(a: Double, b: Double) = func(a, b)
    }
  }

  def newTempFileName(): String = {
    tempFileCounter += 1
    "gilbert" + tempFileCounter + ".output"
  }

  def execute(executable: Executable): Any = {
    executable match {

      case executable: WriteMatrix => {
        handle[WriteMatrix, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (_, matrix) =>
            {
              val tempFileName = newTempFileName()
              matrix.write("file:///" + tempFileName, DelimitedOutputFormat(Submatrix.outputFormatter("\n", " "), ""));
            }
          })
      }

      case executable: WriteString => {
        handle[WriteString, Scalar[String]](
          executable,
          { exec => evaluate[Scalar[String]](exec.string) },
          { (_, string) =>
            {
              val tempFileName = newTempFileName()
              string.write("file:///" + tempFileName, CsvOutputFormat())
            }
          })
      }

      //TODO: Fix
      case executable: WriteFunction => {
        throw new TransformationNotSupportedError("WriteFunction is not supported by Stratosphere")
      }

      //TODO: Fix
      case executable: WriteScalarRef => {
        handle[WriteScalarRef, Scalar[Double]](
          executable,
          { exec => evaluate[Scalar[Double]](exec.scalar) },
          { (_, scalar) =>
            {
              val tempFileName = newTempFileName()
              scalar.write("file:///" + tempFileName, CsvOutputFormat())
            }
          })
        throw new TransformationNotSupportedError("WriteScalarRef is not supported by Stratosphere")
      }

      case VoidExecutable => {
        null
      }

      case executable: ScalarMatrixTransformation => {
        handle[ScalarMatrixTransformation, (Scalar[Double], Matrix)](
          executable,
          { exec => (evaluate[Scalar[Double]](exec.scalar), evaluate[Matrix](exec.matrix)) },
          {
            case (exec, (scalar, matrix)) => {
              exec.operation match {
                case Addition => {
                  scalar cross matrix map { (scalar, submatrix) => submatrix.plus(scalar) }
                }
                case Subtraction => {
                  scalar cross matrix map { (scalar, submatrix) => submatrix.plus(-scalar) }
                }
                case Multiplication => {
                  scalar cross matrix map { (scalar, submatrix) => submatrix.times(scalar) }
                }
                case Division => {
                  scalar cross matrix map { (scalar, submatrix) =>
                    {
                      val result = submatrix.like()
                      result.assign { x: Double => scalar / x }
                    }
                  }
                }
              }
            }
          })
      }

      case executable: MatrixScalarTransformation => {
        handle[MatrixScalarTransformation, (Matrix, Scalar[Double])](
          executable,
          { exec => (evaluate[Matrix](exec.matrix), evaluate[Scalar[Double]](exec.scalar)) },
          {
            case (exec, (matrix, scalar)) => {
              exec.operation match {
                case Addition => {
                  matrix cross scalar map { (submatrix, scalar) => submatrix.plus(scalar) }
                }
                case Subtraction => {
                  matrix cross scalar map { (submatrix, scalar) => submatrix.plus(-scalar) }
                }
                case Multiplication => {
                  matrix cross scalar map { (submatrix, scalar) => submatrix.times(scalar) }
                }
                case Division => {
                  matrix cross scalar map { (submatrix, scalar) => submatrix.divide(scalar) }
                }
              }
            }
          })
      }

      case executable: ScalarScalarTransformation => {
        handle[ScalarScalarTransformation, (Scalar[Double], Scalar[Double])](
          executable,
          { exec => (evaluate[Scalar[Double]](exec.left), evaluate[Scalar[Double]](exec.right)) },
          {
            case (exec, (left, right)) => {
              exec.operation match {
                case Addition => {
                  left cross right map { (left, right) => left + right }
                }
                case Subtraction => {
                  left cross right map { (left, right) => left - right }
                }
                case Multiplication => {
                  left cross right map { (left, right) => left * right }
                }
                case Division => {
                  left cross right map { (left, right) => left / right }
                }
                case Maximum => {
                  left union right combinableReduceAll { elements => elements.max }
                }
                case Minimum => {
                  left union right combinableReduceAll { elements => elements.min }
                }
              }
            }
          })
      }

      case executable: AggregateMatrixTransformation => {
        handle[AggregateMatrixTransformation, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (exec, matrix) =>
            {
              exec.operation match {
                case Maximum => {
                  matrix map { x => x.aggregate(Functions.MAX, Functions.IDENTITY) } combinableReduceAll
                    { elements => elements.max }
                }
                case Minimum => {
                  matrix map { x => x.aggregate(Functions.MIN, Functions.IDENTITY) } combinableReduceAll
                    { elements => elements.min }
                }
                case Norm2 => {
                  matrix map { x => x.aggregate(Functions.PLUS, Functions.SQUARE) } combinableReduceAll
                    { x => x.fold(0.0)(_ + _) } map
                    { x => math.sqrt(x) }
                }
              }
            }
          })
      }

      case executable: UnaryScalarTransformation => {
        handle[UnaryScalarTransformation, Scalar[Double]](
          executable,
          { exec => evaluate[Scalar[Double]](exec.scalar) },
          { (exec, scalar) =>
            {
              exec.operation match {
                case Minus => {
                  scalar map { x => -x }
                }
                case Binarize => {
                  scalar map { x => CellwiseFunctions.binarize(x) }
                }
              }
            }
          })
      }

      case executable: scalar => {
        handle[scalar, Unit](
          executable,
          { _ => },
          { (exec, _) => LiteralDataSource(exec.value, LiteralInputFormat[Double]()) })
      }

      case executable: string => {
        handle[string, Unit](
          executable,
          { _ => },
          { (exec, _) => LiteralDataSource(exec.value, LiteralInputFormat[String]()) })
      }

      case executable: CellwiseMatrixTransformation => {
        handle[CellwiseMatrixTransformation, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (exec, matrix) =>
            {
              exec.operation match {
                case Minus => {
                  matrix map { submatrix => submatrix.times(-1) }
                }
                case Binarize => {
                  matrix map { submatrix =>
                    {
                      val result = submatrix.clone()
                      result.assign(CellwiseFunctions.binarize)
                    }
                  }
                }
              }
            }
          })
      }

      case executable: CellwiseMatrixMatrixTransformation =>
        {
          handle[CellwiseMatrixMatrixTransformation, (Matrix, Matrix)](
            executable,
            { exec => (evaluate[Matrix](exec.left), evaluate[Matrix](exec.right)) },
            {
              case (exec, (left, right)) => {
                exec.operation match {
                  case Addition => {
                    left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) => left.plus(right) }
                  }
                  case Subtraction => {
                    left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) => left.minus(right) }
                  }
                  case Multiplication => {
                    left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) =>
                        val result = left.clone()
                        result.assign(right, Functions.MULT)
                      }
                  }
                  case Division => {
                    left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) =>
                        val result = left.clone()
                        result.assign(right, Functions.DIV)
                      }
                  }
                  case Maximum => {
                    left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) =>
                        val result = left.clone()
                        result.assign(right, Functions.MAX)
                      }
                  }
                  case Minimum => {
                    left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) =>
                        val result = left.clone()
                        result.assign(right, Functions.MIN)
                      }
                  }
                }
              }
            })
        }

      case executable: MatrixMult => {
        handle[MatrixMult, (Matrix, Matrix)](
          executable,
          { exec => (evaluate[Matrix](exec.left), evaluate[Matrix](exec.right)) },
          {
            case (_, (left, right)) => {
              left join right where { leftElement => leftElement.columnIndex } isEqualTo
                { rightElement => rightElement.rowIndex } map
                { (left, right) => left.times(right) } groupBy
                { element => (element.rowIndex, element.columnIndex) } combinableReduceGroup
                { elements =>
                  {
                    val element = elements.next.clone()
                    elements.foldLeft(element)({ _ plus _ })
                  }
                }
            }
          })
      }

      case executable: Transpose => {
        handle[Transpose, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (_, matrix) =>
            {
              matrix map {
                case Submatrix(matrix, rowIdx, columnIdx, rowOffset, columnOffset, numTotalRows, numTotalColumns) =>
                  Submatrix(matrix.transpose(), columnIdx, rowIdx, columnOffset, rowOffset, numTotalColumns,
                    numTotalRows)
              }
            }
          })
      }

      case executable: VectorwiseMatrixTransformation => {
        handle[VectorwiseMatrixTransformation, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (exec, matrix) =>
            {
              exec.operation match {
                case NormalizeL1 => {
                  matrix map { submatrix =>
                    submatrix.aggregateRows(VectorFunctions.l1Norm)
                  } groupBy (subvector => subvector.index) combinableReduceGroup {
                    subvectors =>
                      {
                        val firstElement = subvectors.next.clone()
                        subvectors.foldLeft(firstElement)(_ plus _)
                      }
                  } join
                    matrix where { l1norm => l1norm.index } isEqualTo { submatrix => submatrix.rowIndex } map
                    { (l1norm, submatrix) =>
                      val result = submatrix.like()
                      for (rowDenominator <- l1norm.all()) {
                        result.assignRow(rowDenominator.index(),
                          submatrix.viewRow(rowDenominator.index()).divide(rowDenominator.get()))
                      }
                      result
                    }
                }
                case Maximum => {
                  matrix map { submatrix => submatrix.aggregateRows(VectorFunctions.maxVector) } groupBy
                    { subvector => subvector.index } combinableReduceGroup { subvectors =>
                      val firstElement = subvectors.next.clone()
                      subvectors.foldLeft(firstElement) { (left, right) => left.assign(right, VectorFunctions.max) }
                    } map { subvector =>
                      Submatrix(subvector.cross(StratosphereExecutor.ONEVECTOR),
                        subvector.index, 0, subvector.offset, 0, subvector.numTotalEntries, 1)
                    }
                }
                case Minimum => {
                  matrix map { submatrix => submatrix.aggregateRows(VectorFunctions.minVector) } groupBy
                    { subvector => subvector.index } combinableReduceGroup { subvectors =>
                      val firstElement = subvectors.next.clone()
                      subvectors.foldLeft(firstElement) { (left, right) => left.assign(right, VectorFunctions.min) }
                    } map { subvector =>
                      Submatrix(subvector.cross(StratosphereExecutor.ONEVECTOR),
                        subvector.index, 0, subvector.offset, 0, subvector.numTotalEntries, 1)
                    }
                }
                case Norm2 => {
                  matrix map { submatrix =>
                    val temp = submatrix.clone()
                    temp.assign(Functions.SQUARE)
                    temp.aggregateRows(VectorFunctions.sum)
                  } groupBy { subvector => subvector.index } combinableReduceGroup { subvectors =>
                    val firstElement = subvectors.next.clone()
                    subvectors.foldLeft(firstElement) { _ plus _ }
                  } map {
                    subvector =>
                      Submatrix(subvector.cross(StratosphereExecutor.ONEVECTOR), subvector.index,
                        0, subvector.offset, 0, subvector.numTotalEntries, 1)
                  }
                }

              }
            }
          })
      }

      case executable: LoadMatrix => {
        handle[LoadMatrix, (Scalar[String], Scalar[Double], Scalar[Double])](
          executable,
          { exec =>
            (evaluate[Scalar[String]](exec.path), evaluate[Scalar[Double]](exec.numRows),
              evaluate[Scalar[Double]](exec.numColumns))
          },
          {
            case (_, (path, rows, cols)) => {
              if (!path.contract.isInstanceOf[LiteralDataSource[String]]) {
                throw new IllegalArgumentError("Path for LoadMatrix has to be a literal.")
              }

              if (!rows.contract.isInstanceOf[LiteralDataSource[Double]]) {
                throw new IllegalArgumentError("Rows for LoadMatrix has to be a literal.")
              }

              if (!cols.contract.isInstanceOf[LiteralDataSource[Double]]) {
                throw new IllegalArgumentError("Cols for LoadMatrix has to be a literal.")
              }

              val pathLiteral = path.contract.asInstanceOf[LiteralDataSource[String]].values.head
              val rowLiteral = rows.contract.asInstanceOf[LiteralDataSource[Double]].values.head.toInt
              val columnLiteral = cols.contract.asInstanceOf[LiteralDataSource[Double]].values.head.toInt

              val source = DataSource("file:///" + pathLiteral, CsvInputFormat[(Int, Int, Double)]("\n", ' '))

              val partitionPlan = new SquareBlockPartitionPlan(Submatrix.BLOCKSIZE, rowLiteral, columnLiteral)
              val blocks = LiteralDataSource(0, LiteralInputFormat[Int]()) flatMap { _ =>
                for (partition <- partitionPlan.iterator) yield {
                  (partition.id, Submatrix(partition))
                }
              }
              source map {
                case (row, column, value) =>
                  (partitionPlan.partitionId(row, column), row, column, value)
              } cogroup blocks where { entry => entry._1 } isEqualTo { block => block._1 } map { (entries, blocks) =>
                if (blocks.size != 1) {
                  throw new IllegalArgumentError("LoadMatrix coGroup phase can have only one block each")
                }

                val submatrix = blocks.next._2

                for (entry <- entries) {
                  submatrix.setQuick(entry._2, entry._3, entry._4)
                }

                submatrix
              }
            }
          })
      }

      case executable: ones => {
        handle[ones, (Scalar[Double], Scalar[Double])](
          executable,
          { exec => (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numColumns)) },
          {
            case (_, (rows, columns)) => {
              rows cross columns flatMap { (rows, columns) =>
                val partitionPlan = new SquareBlockPartitionPlan(Submatrix.BLOCKSIZE, rows.toInt, columns.toInt)

                for (matrixPartition <- partitionPlan.iterator) yield {
                  if (matrixPartition.rowIndex == matrixPartition.columnIndex) {
                    val matrix = Submatrix(matrixPartition, matrixPartition.numRows)
                    matrix.viewDiagonal().assign(1.0)
                    matrix
                  } else {
                    Submatrix(matrixPartition)
                  }
                }
              }
            }
          })
      }

      case executable: randn => {
        handle[randn, (Scalar[Double], Scalar[Double], Scalar[Double], Scalar[Double])](
          executable,
          { exec =>
            (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numColumns),
              evaluate[Scalar[Double]](exec.mean), evaluate[Scalar[Double]](exec.std))
          },
          {
            case (_, (rows, cols, mean, std)) => {
              rows cross cols map { (rows, cols) => (rows, cols) } cross mean map
                { case ((rows, cols), mean) => (rows, cols, mean) } cross std flatMap
                {
                  case ((rows, cols, mean), std) =>
                    val sampler = new Normal(mean, std)
                    val partitionPlan = new SquareBlockPartitionPlan(Submatrix.BLOCKSIZE, rows.toInt, cols.toInt)

                    for (partition <- partitionPlan.iterator) yield {
                      val result = Submatrix(partition)
                      result.assign { x: Double => sampler.sample() }
                    }
                }
            }
          })
      }

      case executable: spones => {
        handle[spones, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (_, matrix) =>
            {
              matrix map { submatrix =>
                val result = submatrix.clone()
                result.assign(CellwiseFunctions.binarize)
              }
            }
          })
      }

      case executable: sumRow => {
        handle[sumRow, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (_, matrix) =>
            {
              matrix map { submatrix => submatrix.aggregateRows(VectorFunctions.sum) } groupBy
                { subvector => subvector.index } combinableReduceGroup
                { subvectors =>
                  val firstSubvector = subvectors.next.clone()
                  subvectors.foldLeft(firstSubvector)(_ plus _)
                } map
                { subvector =>
                  Submatrix(subvector.cross(StratosphereExecutor.ONEVECTOR), subvector.index, 0, subvector.offset, 0,
                    subvector.numTotalEntries, 1)
                }
            }
          })
      }

      case executable: sumCol => {
        handle[sumCol, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (_, matrix) =>
            {
              matrix map { submatrix => submatrix.aggregateColumns(VectorFunctions.sum) } groupBy
                { subvector => subvector.index } combinableReduceGroup
                { subvectors =>
                  val firstSubvector = subvectors.next.clone()
                  subvectors.foldLeft(firstSubvector)(_ plus _)
                } map {
                  subvector =>
                    Submatrix(StratosphereExecutor.ONEVECTOR.cross(subvector), 0, subvector.index, 0,
                      subvector.offset, 1, subvector.numTotalEntries)
                }
            }
          })
      }

      case executable: diag => {
        handle[diag, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (exec, matrix) =>
            {
              (exec.rows, exec.cols) match {
                case (Some(1), _) => {
                  val entries = matrix map
                    { submatrix => (submatrix.columnIndex, submatrix.numCols, submatrix.columnOffset) }
                  entries cross matrix map {
                    case ((rowIndex, numRows, rowOffset), submatrix) =>
                      val partition = Partition(-1, rowIndex, submatrix.columnIndex, numRows, submatrix.numCols,
                        rowOffset, submatrix.columnOffset, submatrix.numTotalColumns, submatrix.numTotalColumns)
                      val result = Submatrix(partition)

                      if (submatrix.columnIndex == rowIndex) {
                        for (index <- 0 until submatrix.numCols) {
                          result.setQuick(index, index, submatrix.getQuick(0, index))
                        }
                      }

                      result
                  }
                }
                case (_, Some(1)) => {
                  matrix map { submatrix => (submatrix.rowIndex, submatrix.numRows, submatrix.rowOffset) } cross
                    matrix map {
                      case ((columnIndex, numColumns, columnOffset), submatrix) =>
                        val partition = Partition(-1, submatrix.rowIndex, columnIndex, submatrix.numRows, numColumns,
                          submatrix.rowOffset, columnOffset, submatrix.numTotalRows, submatrix.numTotalRows)

                        val result = Submatrix(partition)

                        if (submatrix.rowIndex == columnIndex) {
                          for (index <- 0 until submatrix.numRows) {
                            result.setQuick(index, index, submatrix.getQuick(index, 0))
                          }
                        }

                        result
                    }
                }
                case _ => {
                  val partialDiagResults = matrix map { submatrix =>
                    val partition = Partition(-1, submatrix.rowIndex, 0, submatrix.numRows, 1, submatrix.rowOffset, 0,
                      submatrix.numTotalRows, 1)

                    val result = Submatrix(partition)

                    val rowStart = submatrix.rowOffset
                    val rowEnd = submatrix.rowOffset + submatrix.numRows
                    val columnStart = submatrix.columnOffset
                    val columnEnd = submatrix.columnOffset + submatrix.numCols

                    var indexStart = (-1, -1)
                    var indexEnd = (-1, -1)

                    if (rowStart <= columnStart && rowEnd > columnStart) {
                      indexStart = (columnStart - rowStart, 0)
                    }

                    if (columnStart < rowStart && columnEnd > rowStart) {
                      indexStart = (0, rowStart - columnStart)
                    }

                    if (rowStart < columnEnd && rowEnd >= columnEnd) {
                      indexEnd = (columnEnd - rowStart, submatrix.numCols)
                    }

                    if (columnStart < rowEnd && columnEnd > rowEnd) {
                      indexEnd = (submatrix.numRows, rowEnd - columnStart)
                    }

                    if (indexStart._1 != -1 && indexStart._2 != -1 && indexEnd._1 != -1 && indexEnd._2 != -1) {
                      for (counter <- 0 until indexEnd._1 - indexStart._1) {
                        result.setQuick(counter + indexStart._1, 0, submatrix.getQuick(indexStart._1 + counter,
                          indexStart._2 + counter))
                      }
                    } else {
                      assert(indexStart._1 == -1 && indexStart._2 == -1 && indexEnd._1 == -1 && indexEnd._2 == -1)
                    }

                    result
                  }

                  partialDiagResults groupBy { partialResult => partialResult.rowIndex } combinableReduceGroup {
                    results =>
                      val result = results.next.copy()
                      results.foldLeft(result)(_ plus _)
                  }
                }
              }
            }
          })
      }

      case executable: FixpointIteration => {
        handle[FixpointIteration, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.initialState) },
          { (exec, initialState) =>
            val numberIterations = 10
            val stepFunction = { partialSolution: Matrix =>
              val oldStatePlaceholderValue = iterationStatePlaceholderValue
              iterationStatePlaceholderValue = Some(partialSolution)
              val result = evaluate[Matrix](exec.updatePlan)
              iterationStatePlaceholderValue = oldStatePlaceholderValue
              result
            }
            initialState.iterate(numberIterations, stepFunction);
          })
      }

      case IterationStatePlaceholder => {
        iterationStatePlaceholderValue match {
          case Some(value) => value
          case None => throw new StratosphereExecutionError("The iteration state placeholder value was not set yet.")
        }
      }

      case executable: sum => {
        handle[sum, (Matrix, Scalar[Double])](
          executable,
          { exec => (evaluate[Matrix](exec.matrix), evaluate[Scalar[Double]](exec.dimension)) },
          {
            case (_, (matrix, scalar)) =>
              scalar cross matrix map { (scalar, submatrix) =>
                if (scalar == 1) {
                  (submatrix.columnIndex, scalar, submatrix.aggregateColumns(VectorFunctions.sum))
                } else {
                  (submatrix.rowIndex, scalar, submatrix.aggregateRows(VectorFunctions.sum))
                }
              } groupBy { case (group, scalar, subvector) => group } combinableReduceGroup { subvectors =>
                val firstSubvector = subvectors.next
                (firstSubvector._1, firstSubvector._2, subvectors.foldLeft(firstSubvector._3.clone())(_ plus _._3))
              } map {
                case (group, scalar, subvector) =>
                  if(scalar == 1){
                    Submatrix(StratosphereExecutor.ONEVECTOR.cross(subvector),0, subvector.index, 0,
                        subvector.offset,1, subvector.numTotalEntries)
                  }else{
                    Submatrix(subvector.cross(StratosphereExecutor.ONEVECTOR), subvector.index, 0,
                        subvector.offset, 0, subvector.numTotalEntries, 1)
                  }
              }
          })
      }

      case function: function => {
        throw new StratosphereExecutionError("Cannot execute function. Needs function application")
      }

      case compound: CompoundExecutable => {
        val executables = compound.executables map { evaluate[ScalaSink[_]](_) }
        new ScalaPlan(executables)
      }

      case parameter: StringParameter => {
        throw new StratosphereExecutionError("Parameter found. Cannot execute parameters.")
      }

      case parameter: ScalarParameter => {
        throw new StratosphereExecutionError("Parameter found. Cannot execute parameters.")
      }

      case parameter: MatrixParameter => {
        throw new StratosphereExecutionError("Parameter found. Cannot execute parameters.")
      }

      case parameter: FunctionParameter => {
        throw new StratosphereExecutionError("Parameter found. Cannot execute parameters.")
      }

    }
  }

}

object StratosphereExecutor {
  val ONEVECTOR = new DenseVector(Array(1.))
}