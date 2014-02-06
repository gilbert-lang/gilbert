package org.gilbertlang.runtime.execution.stratosphere

import org.gilbertlang.runtime.Executor
import eu.stratosphere.api.scala.operators.CsvInputFormat
import eu.stratosphere.api.scala.DataSource
import eu.stratosphere.api.scala.DataSet
import eu.stratosphere.api.scala.operators.CsvOutputFormat
import org.gilbertlang.runtimeMacros.io.LiteralInputFormat
import eu.stratosphere.api.scala.operators.DelimitedOutputFormat
import scala.collection.convert.WrapAsScala
import scala.collection.mutable.ArrayBuilder
import eu.stratosphere.api.scala.ScalaPlan
import eu.stratosphere.api.scala.ScalaSink
import org.gilbertlang.runtime.Operations._
import org.gilbertlang.runtime.Executables._
import org.gilbertlang.runtimeMacros.linalg.Submatrix
import org.gilbertlang.runtimeMacros.linalg.Partition
import org.gilbertlang.runtimeMacros.linalg.numerics
import org.gilbertlang.runtime.execution.CellwiseFunctions
import breeze.linalg.norm
import breeze.linalg.*
import org.gilbertlang.runtimeMacros.linalg.Subvector
import org.gilbertlang.runtimeMacros.linalg.SquareBlockPartitionPlan
import org.gilbertlang.runtimeMacros.linalg.Configuration

class StratosphereExecutor extends Executor with WrapAsScala {
  type Entry = Submatrix
  type Matrix = DataSet[Entry]
  type Scalar[T] = DataSet[T]
  private var tempFileCounter = 0
  private var iterationStatePlaceholderValue: Option[Matrix] = None
  
  def getCWD: String = System.getProperty("user.dir")

  def newTempFileName(): String = {
    tempFileCounter += 1
    "file://" + getCWD + "/gilbert" + tempFileCounter + ".output"
  }

  def execute(executable: Executable): Any = {
    executable match {

      case executable: WriteMatrix => {
        handle[WriteMatrix, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (_, matrix) =>
            {
              val completePathWithFilename = newTempFileName()
              matrix.write(completePathWithFilename, DelimitedOutputFormat(Submatrix.outputFormatter("\n", " "), ""));
            }
          })
      }

      case executable: WriteString => {
        handle[WriteString, Scalar[String]](
          executable,
          { exec => evaluate[Scalar[String]](exec.string) },
          { (_, string) =>
            {
              val completePathWithFilename = newTempFileName()
              string.write(completePathWithFilename, CsvOutputFormat())
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
              val completePathWithFilename = newTempFileName()
              scalar.write(completePathWithFilename, CsvOutputFormat())
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
                  scalar cross matrix map { (scalar, submatrix) => submatrix + scalar }
                }
                case Subtraction => {
                  scalar cross matrix map { (scalar, submatrix) => submatrix + -scalar }
                }
                case Multiplication => {
                  scalar cross matrix map { (scalar, submatrix) => submatrix * scalar }
                }
                case Division => {
                  scalar cross matrix map { (scalar, submatrix) =>
                    {
                      val partition = submatrix.getPartition
                      val result = Submatrix.init(partition, scalar)
                      result / submatrix
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
                  matrix cross scalar map { (submatrix, scalar) => submatrix + scalar }
                }
                case Subtraction => {
                  matrix cross scalar map { (submatrix, scalar) => submatrix - scalar }
                }
                case Multiplication => {
                  matrix cross scalar map { (submatrix, scalar) => submatrix * scalar }
                }
                case Division => {
                  matrix cross scalar map { (submatrix, scalar) => submatrix / scalar }
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
                  matrix map { x => x.max } combinableReduceAll
                    { elements => elements.max }
                }
                case Minimum => {
                  matrix map { x => x.min } combinableReduceAll
                    { elements => elements.min }
                }
                case Norm2 => {
                  matrix map { x => breeze.linalg.sum(x:*x) } combinableReduceAll
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
                  matrix map { submatrix => submatrix * -1.0}
                }
                case Binarize => {
                  matrix map { submatrix =>
                    {
                      submatrix.mapActiveValues(x => CellwiseFunctions.binarize(x))
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
                      { (left, right) => left + right }
                  }
                  case Subtraction => {
                    left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) => left - right }
                  }
                  case Multiplication => {
                    left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) => left :* right
                      }
                  }
                  case Division => {
                    left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) => left / right
                      }
                  }
                  case Maximum => {
                    left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) =>
                       numerics.max(left, right)
                      }
                  }
                  case Minimum => {
                    left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) =>
                        numerics.min(left, right)
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
                { (left, right) => left * right } groupBy
                { element => (element.rowIndex, element.columnIndex) } combinableReduceGroup
                { elements =>
                  {
                    val element = elements.next.copy
                    elements.foldLeft(element)({ _ + _ })
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
                  Submatrix(matrix.t, columnIdx, rowIdx, columnOffset, rowOffset, numTotalColumns,
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
                    norm(submatrix( * , ::),1)
                  } groupBy (subvector => subvector.index) combinableReduceGroup {
                    subvectors =>
                      {
                        val firstElement = subvectors.next.copy
                        subvectors.foldLeft(firstElement)(_ + _)
                      }
                  } join
                    matrix where { l1norm => l1norm.index } isEqualTo { submatrix => submatrix.rowIndex } map
                    { (l1norm, submatrix) =>
                      val result = submatrix.copy
                      for(col <- 0 until submatrix.cols)
                        result(::, col) :/= l1norm
                        
                      result
                    }
                }
                case Maximum => {
                  matrix map { submatrix => numerics.max(submatrix(*, ::)) } groupBy
                    { subvector => subvector.index } combinableReduceGroup { subvectors =>
                      val firstElement = subvectors.next.copy
                      subvectors.foldLeft(firstElement) { numerics.max(_, _) }
                    } map { subvector => subvector.asMatrix }
                }
                case Minimum => {
                  matrix map { submatrix => numerics.min(submatrix(*, ::)) } groupBy
                    { subvector => subvector.index } combinableReduceGroup { subvectors =>
                      val firstElement = subvectors.next.copy
                      subvectors.foldLeft(firstElement) { numerics.min(_, _) }
                    } map { subvector => subvector.asMatrix }
                }
                case Norm2 => {
                  matrix map { submatrix =>
                    norm(submatrix( *, ::),2)
                  } groupBy { subvector => subvector.index } combinableReduceGroup { subvectors =>
                    val firstElement = subvectors.next.copy
                    subvectors.foldLeft(firstElement) { _ + _ }
                  } map { subvector => subvector.asMatrix  }
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

              val source = DataSource("file://" + pathLiteral, CsvInputFormat[(Int, Int, Double)]("\n", ' '))
              
              val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rowLiteral, columnLiteral)
             
              val blocks = LiteralDataSource(0, LiteralInputFormat[Int]()) flatMap { _ =>
                for (partition <- partitionPlan.iterator) yield {
                  (partition.id, partition)
                }
              }
              source map {
                case (row, column, value) =>
                  (partitionPlan.partitionId(row, column), row, column, value)
              } cogroup blocks where { entry => entry._1 } isEqualTo { block => block._1 } map { (entries, blocks) =>
                if(!blocks.hasNext){
                  throw new IllegalArgumentError("LoadMatrix coGroup phase must have at least one block")
                }
                
                val partition = blocks.next._2
                
                if (blocks.hasNext) {
                  throw new IllegalArgumentError("LoadMatrix coGroup phase must have at most one block")
                }
                
                
                Submatrix(partition, (entries map { case (id, row, col, value) => (row, col, value)}).toSeq)
              }
            }
          })
      }
  
      case compound: CompoundExecutable => {
        val executables = compound.executables map { evaluate[ScalaSink[_]](_) }
        new ScalaPlan(executables)
      }

      
      case executable: ones => {
        handle[ones, (Scalar[Double], Scalar[Double])](
          executable,
          { exec => (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numColumns)) },
          {
            case (_, (rows, columns)) => {
              rows cross columns flatMap { (rows, columns) =>
                val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows.toInt, columns.toInt)

                for (matrixPartition <- partitionPlan.iterator) yield {
                  Submatrix.init(matrixPartition, 1.0)
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
                    val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows.toInt, cols.toInt)

                    for (partition <- partitionPlan.iterator) yield {
                      Submatrix.rand(partition)
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
                submatrix.mapActiveValues(x => CellwiseFunctions.binarize(x))
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
              matrix map { submatrix => breeze.linalg.sum(submatrix(*, ::)) } groupBy
                { subvector => subvector.index } combinableReduceGroup
                { subvectors =>
                  val firstSubvector = subvectors.next.copy
                  subvectors.foldLeft(firstSubvector)(_ + _)
                } map
                { subvector => subvector.asMatrix }
            }
          })
      }

      case executable: sumCol => {
        handle[sumCol, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (_, matrix) =>
            {
              matrix map { submatrix => breeze.linalg.sum(submatrix(::, *)) } groupBy
                { submatrix => submatrix.columnIndex } combinableReduceGroup
                { subvectors =>
                  val firstSubvector = subvectors.next.copy
                  subvectors.foldLeft(firstSubvector)(_ + _)
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
                    { submatrix => (submatrix.columnIndex, submatrix.cols, submatrix.columnOffset) }
                  entries cross matrix map {
                    case ((rowIndex, numRows, rowOffset), submatrix) =>
                      val partition = Partition(-1, rowIndex, submatrix.columnIndex, numRows, submatrix.cols,
                        rowOffset, submatrix.columnOffset, submatrix.totalColumns, submatrix.totalColumns)
                      val result = Submatrix(partition, submatrix.totalColumns)

                      if (submatrix.columnIndex == rowIndex) {
                        for (index <- 0 until submatrix.cols) {
                          result.update(index, index, submatrix(0, index))
                        }
                      }

                      result
                  }
                }
                case (_, Some(1)) => {
                  matrix map { submatrix => (submatrix.rowIndex, submatrix.rows, submatrix.rowOffset) } cross
                    matrix map {
                      case ((columnIndex, numColumns, columnOffset), submatrix) =>
                        val partition = Partition(-1, submatrix.rowIndex, columnIndex, submatrix.rows, numColumns,
                          submatrix.rowOffset, columnOffset, submatrix.totalRows, submatrix.totalRows)

                        val result = Submatrix(partition)

                        if (submatrix.rowIndex == columnIndex) {
                          for (index <- 0 until submatrix.rows) {
                            result.update(index, index, submatrix(index, 0))
                          }
                        }

                        result
                    }
                }
                case _ => {
                  val partialDiagResults = matrix map { submatrix =>
                    val partition = Partition(-1, submatrix.rowIndex, 0, submatrix.rows, 1, submatrix.rowOffset, 0,
                      submatrix.totalRows, 1)

                    val result = Submatrix(partition, submatrix.rows)

                    val rowStart = submatrix.rowOffset
                    val rowEnd = submatrix.rowOffset + submatrix.rows
                    val columnStart = submatrix.columnOffset
                    val columnEnd = submatrix.columnOffset + submatrix.cols

                    var indexStart = (-1, -1)
                    var indexEnd = (-1, -1)

                    if (rowStart <= columnStart && rowEnd > columnStart) {
                      indexStart = (columnStart - rowStart, 0)
                    }

                    if (columnStart < rowStart && columnEnd > rowStart) {
                      indexStart = (0, rowStart - columnStart)
                    }

                    if (rowStart < columnEnd && rowEnd >= columnEnd) {
                      indexEnd = (columnEnd - rowStart, submatrix.cols)
                    }

                    if (columnStart < rowEnd && columnEnd > rowEnd) {
                      indexEnd = (submatrix.rows, rowEnd - columnStart)
                    }

                    if (indexStart._1 != -1 && indexStart._2 != -1 && indexEnd._1 != -1 && indexEnd._2 != -1) {
                      for (counter <- 0 until indexEnd._1 - indexStart._1) {
                        result.update(counter + indexStart._1, 0, submatrix(indexStart._1 + counter,
                          indexStart._2 + counter))
                      }
                    } else {
                      assert(indexStart._1 == -1 && indexStart._2 == -1 && indexEnd._1 == -1 && indexEnd._2 == -1)
                    }

                    result
                  }

                  partialDiagResults groupBy { partialResult => partialResult.rowIndex } combinableReduceGroup {
                    results =>
                      val result = results.next.copy
                      results.foldLeft(result)(_ + _)
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
                  (submatrix.columnIndex, breeze.linalg.sum(submatrix(::, *)))
                } else {
                  (submatrix.rowIndex, breeze.linalg.sum(submatrix(*, ::)).asMatrix)
                }
              } groupBy { case (group, subvector) => group } combinableReduceGroup { submatrices =>
                val firstSubvector = submatrices.next
                (firstSubvector._1, submatrices.foldLeft(firstSubvector._2.copy)(_ + _._2))
              } map { case (_, submatrix) => submatrix}
          })
      }

      case function: function => {
        throw new StratosphereExecutionError("Cannot execute function. Needs function application")
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