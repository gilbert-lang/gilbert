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

class StratosphereExecutor extends Executor {
  private var tempFileCounter = 0
  type Entry = (Int, Int, Double)
  type Matrix = DataSet[Entry]
  type Scalar[T] = DataSet[T]

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
              matrix.write("file:///" + tempFileName, CsvOutputFormat("\n", " "));
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
                  scalar cross matrix map { case (scalar, (row, col, value)) => (row, col, scalar + value) }
                }
                case Subtraction => {
                  scalar cross matrix map { case (scalar, (row, col, value)) => (row, col, scalar - value) }
                }
                case Multiplication => {
                  scalar cross matrix map { case (scalar, (row, col, value)) => (row, col, scalar * value) }
                }
                case Division => {
                  scalar cross matrix map { case (scalar, (row, col, value)) => (row, col, scalar / value) }
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
                  matrix cross scalar map { case ((row, col, value), scalar) => (row, col, scalar + value) }
                }
                case Subtraction => {
                  matrix cross scalar map { case ((row, col, value), scalar) => (row, col, value - scalar) }
                }
                case Multiplication => {
                  matrix cross scalar map { case ((row, col, value), scalar) => (row, col, value * scalar) }
                }
                case Division => {
                  matrix cross scalar map { case ((row, col, value), scalar) => (row, col, value / scalar) }
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
                  matrix map { x => x._3 } combinableReduceAll { elements => elements.max }
                }
                case Minimum => {
                  matrix map { x => x._3 } combinableReduceAll { elements => elements.min }
                }
                case Norm2 => {
                  matrix map { x => x._3 * x._3 } combinableReduceAll { x => x.fold(0.0)(_ + _) } map
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
                  matrix map { case (row, col, value) => (row, col, -value) }
                }
                case Binarize => {
                  matrix map { case (row, col, value) => (row, col, CellwiseFunctions.binarize(value)) }
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
                    left join right where { x => (x._1, x._2) } isEqualTo { y => (y._1, y._2) } map
                      { (left, right) => (left._1, left._2, left._3 + right._3) }
                  }
                  case Subtraction => {
                    left join right where { x => (x._1, x._2) } isEqualTo { y => (y._1, y._2) } map
                      { (left, right) => (left._1, left._2, left._3 - right._3) }
                  }
                  case Multiplication => {
                    left join right where { x => (x._1, x._2) } isEqualTo { y => (y._1, y._2) } map
                      { (left, right) => (left._1, left._2, left._3 * right._3) }
                  }
                  case Division => {
                    left join right where { x => (x._1, x._2) } isEqualTo { y => (y._1, y._2) } map
                      { (left, right) => (left._1, left._2, left._3 / right._3) }
                  }
                  case Maximum => {
                    left join right where { x => (x._1, x._2) } isEqualTo { y => (y._1, y._2) } map
                      { (left, right) => (left._1, left._2, math.max(left._3, right._3)) }
                  }
                  case Minimum => {
                    left join right where { x => (x._1, x._2) } isEqualTo { y => (y._1, y._2) } map
                      { (left, right) => (left._1, left._2, math.min(left._3, right._3)) }
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
              left join right where { leftElement => leftElement._2 } isEqualTo { rightElement => rightElement._1 } map
                { (left, right) => (left._1, right._2, left._3 * right._3) } groupBy
                { element => (element._1, element._2) } combinableReduceGroup
                { elements =>
                  {
                    val element = elements.next
                    (element._1, element._2, (elements map { x => x._3 }).foldLeft(element._3)({ _ + _ }))
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
              matrix map { case (row, col, value) => (col, row, value) }
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
                  matrix groupBy (element => (element._1)) combinableReduceGroup { elements =>
                    val element = elements.next
                    val row = element._1
                    val value = element._3
                    (row, 0, (elements map { x => math.abs(x._3) }).foldLeft(value)(_ + _))
                  } join
                    matrix where { l1norm => l1norm._1 } isEqualTo { element => element._1 } map
                    { (l1norm, element) => (element._1, element._2, element._3 / l1norm._3) }
                }
                case Maximum => {
                  matrix groupBy { element => element._1 } combinableReduceGroup { elements =>
                    val element = elements.next
                    (element._1, 0, (elements map (element => element._3)).max)
                  }
                }
                case Minimum => {
                  matrix groupBy { element => element._1 } combinableReduceGroup { elements =>
                    val element = elements.next
                    (element._1, 0, (elements map (element => element._3)).min)
                  }
                }
                case Norm2 => {
                  matrix groupBy { element => element._1 } combinableReduceGroup { elements =>
                    val element = elements.next
                    (element._1, 0, (elements map
                      { element => element._3 * element._3 }).foldLeft(element._3 * element._3)(_ + _))
                  } map
                    { element => (element._1, element._2, math.sqrt(element._3)) }
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

              DataSource("file:///" + pathLiteral, CsvInputFormat[(Int, Int, Double)]("\n", ' '))

            }
          })
      }

      case executable: ones => {
        handle[ones, (Scalar[Double], Scalar[Double])](
          executable,
          { exec => (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numColumns)) },
          {
            case (_, (rows, columns)) => {
              rows cross columns map { (rows, columns) =>
                for (row <- 0 until rows.toInt; column <- 0 until columns.toInt) yield (row, column, 1)
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
                { case ((rows, cols), mean) => (rows, cols, mean) } cross std map
                {
                  case ((rows, cols, mean), std) =>
                    val sampler = new Normal(mean, std)

                    for (row <- 0 until rows.toInt; col <- 0 until cols.toInt) yield (row, col, sampler.sample())
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
              matrix map { case (row, col, value) => (row, col, CellwiseFunctions.binarize(value)) }
            }
          })
      }

      case executable: sumRow => {
        handle[sumRow, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (_, matrix) =>
            {
              matrix groupBy { element => element._1 } combinableReduceGroup
                { elements =>
                  val firstElement = elements.next
                  (firstElement._1, 0, (elements map { element => element._3 }).foldLeft(firstElement._3)(_ + _))
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
              matrix groupBy { element => element._2 } combinableReduceGroup
                { elements =>
                  val firsElement = elements.next
                  (0, firsElement._2, (elements map (element => element._3)).foldLeft(firsElement._3)(_ + _))
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
                  matrix map { element => (element._2, element._2, element._3) }
                }
                case (_, Some(1)) => {
                  matrix map { element => (element._1, element._1, element._3) }
                }
                case (Some(x), Some(y)) => {
                  matrix flatMap { element =>
                    if (element._1 == element._2) {
                      List((element._1, 0, element._3)).iterator
                    } else {
                      List[(Int, Int, Double)]().iterator
                    }
                  }
                }
                //TODO: Fix
                case (None, None) => {
                  throw new IllegalArgumentError("The size of the matrix argument cannot be retrieved")
                }
              }
            }
          })
      }

    }
  }

}