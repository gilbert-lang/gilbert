package org.gilbertlang.runtime.execution.stratosphere

import org.gilbertlang.runtime.Executor
import eu.stratosphere.api.scala.operators.CsvInputFormat
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators.CsvOutputFormat
import eu.stratosphere.api.scala.operators.DelimitedOutputFormat
import scala.collection.convert.WrapAsScala
import org.gilbertlang.runtime.Operations._
import org.gilbertlang.runtime.Executables._
import org.gilbertlang.runtimeMacros.linalg.Submatrix
import org.gilbertlang.runtimeMacros.linalg.SubmatrixBoolean
import org.gilbertlang.runtimeMacros.linalg.numerics
import org.gilbertlang.runtime.execution.CellwiseFunctions
import breeze.linalg.norm
import breeze.linalg.*
import org.gilbertlang.runtimeMacros.linalg.Configuration
import eu.stratosphere.api.common.operators.Operator
import scala.language.implicitConversions
import scala.language.reflectiveCalls
import org.gilbertlang.runtime.RuntimeTypes._
import scala.collection.mutable
import org.gilbertlang.runtime.Executables.diag
import org.gilbertlang.runtime.Executables.VectorwiseMatrixTransformation
import org.gilbertlang.runtime.Executables.WriteMatrix
import org.gilbertlang.runtime.Executables.MatrixParameter
import scala.Some
import org.gilbertlang.runtime.Executables.eye
import org.gilbertlang.runtime.Executables.CellArrayReferenceCellArray
import org.gilbertlang.runtime.Executables.FunctionParameter
import org.gilbertlang.runtime.Executables.CellArrayReferenceMatrix
import org.gilbertlang.runtime.Executables.WriteFunction
import org.gilbertlang.runtime.Executables.LoadMatrix
import org.gilbertlang.runtime.Executables.randn
import org.gilbertlang.runtime.Executables.CompoundExecutable
import org.gilbertlang.runtime.Executables.UnaryScalarTransformation
import org.gilbertlang.runtime.Executables.scalar
import org.gilbertlang.runtime.Executables.ScalarMatrixTransformation
import org.gilbertlang.runtime.Executables.StringParameter
import org.gilbertlang.runtime.Executables.WriteScalar
import org.gilbertlang.runtime.Executables.zeros
import org.gilbertlang.runtime.Executables.FixpointIteration
import org.gilbertlang.runtime.Executables.ScalarParameter
import org.gilbertlang.runtime.Executables.CellwiseMatrixTransformation
import org.gilbertlang.runtime.Executables.CellArrayReferenceScalar
import org.gilbertlang.runtime.Executables.MatrixMult
import org.gilbertlang.runtime.Executables.boolean
import org.gilbertlang.runtime.Executables.FixpointIterationCellArray
import org.gilbertlang.runtime.Executables.sumCol
import org.gilbertlang.runtime.Executables.string
import org.gilbertlang.runtime.Executables.ScalarScalarTransformation
import org.gilbertlang.runtime.Executables.CellArrayExecutable
import org.gilbertlang.runtimeMacros.linalg.SquareBlockPartitionPlan
import org.gilbertlang.runtime.Executables.sum
import org.gilbertlang.runtime.RuntimeTypes.CellArrayType
import org.gilbertlang.runtime.Executables.spones
import org.gilbertlang.runtime.Executables.ones
import org.gilbertlang.runtime.Executables.AggregateMatrixTransformation
import org.gilbertlang.runtime.Executables.WriteCellArray
import org.gilbertlang.runtime.Executables.CellArrayReferenceString
import org.gilbertlang.runtime.Executables.CellwiseMatrixMatrixTransformation
import org.gilbertlang.runtime.Executables.IterationStatePlaceholderCellArray
import org.gilbertlang.runtime.Executables.MatrixScalarTransformation
import org.gilbertlang.runtime.Executables.CellArrayReferenceFunction
import org.gilbertlang.runtime.Executables.Transpose
import org.gilbertlang.runtimeMacros.linalg.Partition
import org.gilbertlang.runtime.RuntimeTypes.MatrixType
import org.gilbertlang.runtime.Executables.function
import org.gilbertlang.runtime.Executables.sumRow
import org.gilbertlang.runtime.Executables.WriteString
import eu.stratosphere.api.scala.CollectionDataSource
import eu.stratosphere.types.{DoubleValue, IntValue, StringValue}


class StratosphereExecutor extends Executor with WrapAsScala {
  import ImplicitConversions._

  type Matrix = DataSet[Submatrix]
  type BooleanMatrix = DataSet[SubmatrixBoolean]
  type Scalar[T] = DataSet[T]
  type CellArray = DataSet[CellEntry]
  private var tempFileCounter = 0
  private var iterationStatePlaceholderValue: Option[Matrix] = None
  private var iterationStatePlaceholderValueCellArray: Option[CellArray] = None
  private var convergencePreviousStateValue: Option[Matrix] = None
  private var convergenceCurrentStateValue: Option[Matrix] = None
  private var convergenceCurrentStateCellArrayValue: Option[CellArray] = None
  private var convergencePreviousStateCellArrayValue: Option[CellArray] = None
  
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
              List(matrix.write(completePathWithFilename, DelimitedOutputFormat(Submatrix.outputFormatter("\n", " "),
                ""),
                  s"WriteMatrix($completePathWithFilename)"))
            }
          })
      }

      case executable: WriteCellArray => {
        handle[WriteCellArray, CellArray](
        executable,
        { exec => evaluate[CellArray](exec.cellArray)},
        { (exec, cellArray) =>
          var index = 0
          val cellArrayType = exec.cellArray.getType
          val result = new Array[ScalaSink[_]](cellArrayType.elementTypes.length)
          while(index < cellArrayType.elementTypes.length){
            val completePathWithFilename = newTempFileName()
            val loopIndex = index
            val filtered = cellArray filter { x => x.index == loopIndex}
            val sink = cellArrayType.elementTypes(index) match {
              case MatrixType(DoubleType) =>
                val mappedCell = filtered map { x => x.wrappedValue[Submatrix]}
                mappedCell.write(completePathWithFilename, DelimitedOutputFormat(Submatrix.outputFormatter("\n", " "),
                  ""),
                  s"WriteCellArray(Matrix[Double], $completePathWithFilename)")
              case StringType =>
                val mappedCell =filtered map( x => x.wrappedValue[String])
                mappedCell.write(completePathWithFilename, CsvOutputFormat(),
                  s"WriteCellArray(String, $completePathWithFilename)")
              case DoubleType =>
                val mappedCell = filtered map(x => x.wrappedValue[Double])
                mappedCell.write(completePathWithFilename, CsvOutputFormat(), s"WriteCellArray(Double," +
                  s"$completePathWithFilename)")
              case BooleanType =>
                val mappedCell = filtered map(x => x.wrappedValue[Boolean])
                mappedCell.write(completePathWithFilename, CsvOutputFormat(), s"WriteCellArray(Boolean," +
                  s"$completePathWithFilename)")
            }
            result(index) = sink
            index += 1
          }

          result.toList
        }
        )
      }

      case executable: WriteString => {
        handle[WriteString, Scalar[String]](
          executable,
          { exec => evaluate[Scalar[String]](exec.string) },
          { (_, string) =>
            {
              val completePathWithFilename = newTempFileName()
              List(string.write(completePathWithFilename, CsvOutputFormat(), s"WriteString($completePathWithFilename)" +
                s""))
            }
          })
      }

      //TODO: Fix
      case executable: WriteFunction => { 
        throw new TransformationNotSupportedError("WriteFunction is not supported by Stratosphere")
      }

      //TODO: Fix
      case executable: WriteScalar => {
        executable.scalar.getType match {
          case DoubleType =>
            handle[WriteScalar, Scalar[Double]](
            executable,
            { exec => evaluate[Scalar[Double]](exec.scalar) },
            { (_, scalar) =>
            {
              val completePathWithFilename = newTempFileName()
              List(scalar.write(completePathWithFilename, CsvOutputFormat(),
                s"WriteScalarRef($completePathWithFilename)"))
            }
            })
          case BooleanType =>
            handle[WriteScalar, Scalar[Boolean]](
            executable,
            { exec => evaluate[Scalar[Boolean]](exec.scalar) },
            { (_, scalar) =>
              val completePathWithFilename = newTempFileName()
              List(scalar.write(completePathWithFilename, CsvOutputFormat(),
                s"WriteScalarRef($completePathWithFilename)"))
            }
            )

        }


      }

      case VoidExecutable => {
        null
      }

      case executable: ScalarMatrixTransformation => {
        executable.operation match {
          case logicOperation: LogicOperation => {
            handle[ScalarMatrixTransformation, (Scalar[Boolean], BooleanMatrix)](
                executable,
                { exec => (evaluate[Scalar[Boolean]](exec.scalar), evaluate[BooleanMatrix](exec.matrix))},
                { case (_, (scalar, matrix)) =>{
                  logicOperation match {
                    case And => {
                      val result = scalar cross matrix map { (scalar, submatrix) => submatrix :& scalar }
                      result.setName("SM: Logical And")
                      result
                    }
                    case Or => {
                      val result = scalar cross matrix map { (scalar, submatrix) => submatrix :| scalar }
                      result.setName("SM: Logical Or")
                      result
                    }
                  }
                }})
          }
          case operation => {
            handle[ScalarMatrixTransformation, (Scalar[Double], Matrix)](
          executable,
          { exec => (evaluate[Scalar[Double]](exec.scalar), evaluate[Matrix](exec.matrix)) },
          {
            case (_, (scalar, matrix)) => {
              operation match {
                case Addition => {
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix + scalar }
                  result.setName("SM: Addition")
                  result
                }
                case Subtraction => {
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix + -scalar }
                  result.setName("SM: Subtraction")
                  result
                }
                case Multiplication => {
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix * scalar }
                  result.setName("SM: Multiplication")
                  result
                }
                case Division => {
                  val result = scalar cross matrix map { (scalar, submatrix) =>
                    {
                      val partition = submatrix.getPartition
                      val result = Submatrix.init(partition, scalar)
                      result / submatrix
                    }
                  }
                  result.setName("SM: Division")
                  result
                }
                case GreaterThan => {
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix :< scalar }
                  result.setName("SM: Greater than")
                  result
                }
                case GreaterEqualThan => {
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix :<= scalar }
                  result.setName("SM: Greater equal than")
                  result
                }
                case LessThan => {
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix :> scalar }
                  result.setName("SM: Less than")
                  result
                }
                case LessEqualThan => {
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix :>= scalar }
                  result.setName("SM: Less equal than")
                  result
                }
                case Equals => {
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix :== scalar }
                  result.setName("SM: Equals")
                  result
                }
                case NotEquals => {
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix :!= scalar }
                  result.setName("SM: Not equals")
                  result
                } 
              }
            }
          })
          }
        }
      }

      case executable: MatrixScalarTransformation => {
        executable.operation match {
          case logicOperation: LogicOperation => {
            handle[MatrixScalarTransformation, (BooleanMatrix, Scalar[Boolean])](
                executable,
                {exec => (evaluate[BooleanMatrix](exec.matrix), evaluate[Scalar[Boolean]](exec.scalar))},
                { case (_, (matrix, scalar)) => {
                  logicOperation match {
                    case And => {
                      val result = matrix cross scalar map { (submatrix, scalar) => submatrix :& scalar }
                      result.setName("MS: Logical And")
                      result
                    }
                    case Or => {
                      val result = matrix cross scalar map { (submatrix, scalar) => submatrix :| scalar }
                      result.setName("MS: Logical Or")
                      result
                    } 
                  }
                } 
                })
          }
          case operation => {
              handle[MatrixScalarTransformation, (Matrix, Scalar[Double])](
          executable,
          { exec => (evaluate[Matrix](exec.matrix), evaluate[Scalar[Double]](exec.scalar)) },
          {
            case (_, (matrix, scalar)) => {
              operation match {
                case Addition => {
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix + scalar }
                  result.setName("MS: Addition")
                  result
                }
                case Subtraction => {
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix - scalar }
                  result.setName("MS: Subtraction")
                  result
                }
                case Multiplication => {
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix * scalar }
                  result.setName("MS: Multiplication")
                  result
                }
                case Division => {
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix / scalar }
                  result.setName("MS: Division")
                  result
                }
                case GreaterThan => {
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix :> scalar }
                  result.setName("MS: Greater than")
                  result
                }
                case GreaterEqualThan => {
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix :>= scalar }
                  result.setName("MS: Greater equal than")
                  result
                }
                case LessThan => {
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix :< scalar }
                  result.setName("MS: Less than")
                  result
                }
                case LessEqualThan => {
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix :<= scalar }
                  result.setName("MS: Less equal than")
                  result
                }
                case Equals => {
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix :== scalar }
                  result.setName("MS: Equals")
                  result
                }
                case NotEquals => {
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix :!= scalar }
                  result.setName("MS: Not equals")
                  result
                }
              }
            }
          })
          }
        }
      
      }

      case executable: ScalarScalarTransformation => {

        executable.operation match {
          case logicOperation: LogicOperation =>
            handle[ScalarScalarTransformation, (Scalar[Boolean], Scalar[Boolean])](
              executable,
              {exec => (evaluate[Scalar[Boolean]](exec.left), evaluate[Scalar[Boolean]](exec.right))},
              {case (_, (left, right)) => 
                logicOperation match {
                  case And => { 
                    val result = left cross right map { (left, right) => left && right }
                    result.setName("SS: Logical And")
                    result
                  }
                  case Or => { 
                    val result = left cross right map { (left, right) => left || right }
                    result.setName("SS: Logical Or")
                    result
                  }
                }
              }
            )
          case operation =>
            handle[ScalarScalarTransformation, (Scalar[Double], Scalar[Double])](
              executable,
              { exec => (evaluate[Scalar[Double]](exec.left), evaluate[Scalar[Double]](exec.right)) },
              {
                case (_, (left, right)) => {
                  operation match {
                    case Addition => {
                      val result = left cross right map { (left, right) => left + right }
                      result.setName("SS: Addition")
                      result
                    }
                    case Subtraction => {
                      val result = left cross right map { (left, right) => left - right }
                      result.setName("SS: Subtraction")
                      result
                    }
                    case Multiplication => {
                      val result = left cross right map { (left, right) => left * right }
                      result.setName("SS: Multiplication")
                      result
                    }
                    case Division => {
                      val result =left cross right map { (left, right) => left / right }
                      result.setName("SS: Division")
                      result
                    }
                    case Maximum => {
                      val result = left union right combinableReduceAll { elements => elements.max }
                      result.setName("SS: Maximum")
                      result
                    }
                    case Minimum => {
                      val result = left union right combinableReduceAll { elements => elements.min }
                      result.setName("SS: Minimum")
                      result
                    }
                    case GreaterThan => {
                      val result = left cross right map { (left, right) => left > right }
                      result.setName("SS: Greater than")
                      result
                    }
                    case GreaterEqualThan => {
                      val result = left cross right map { (left, right) => left >= right }
                      result.setName("SS: Greater equal than")
                      result
                    }
                    case LessThan => {
                      val result = left cross right map { (left, right) => left < right }
                      result.setName("SS: Less than")
                      result
                    }
                    case LessEqualThan => {
                      val result = left cross right map { (left, right) => left <= right }
                      result.setName("SS: Less equal than")
                      result
                    }
                    case Equals => {
                      val result = left cross right map { (left, right) => left == right}
                      result.setName("SS: Equals")
                      result
                    }
                    case NotEquals => {
                      val result = left cross right map { (left, right) => left != right }
                      result.setName("SS: Not equals")
                      result
                    }
                  }
                }
              })
        }
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
          { (exec, _) => CollectionDataSource(List(exec.value)) })
      }
      
      case executable: boolean => {
        handle[boolean, Unit](
            executable,
            {_ => },
            { (exec, _) => CollectionDataSource(List(exec.value)) })
      }

      case executable: string => {
        handle[string, Unit](
          executable,
          { _ => },
          { (exec, _) => CollectionDataSource(List(exec.value))})
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
          executable.operation match {
            case logicOperation: LogicOperation => {
              handle[CellwiseMatrixMatrixTransformation, (BooleanMatrix, BooleanMatrix)](
                  executable,
                  { exec => (evaluate[BooleanMatrix](exec.left), evaluate[BooleanMatrix](exec.right))},
                  { case (_, (left, right)) => {
                    logicOperation match {
                      case And => {
                        val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                        { y => (y.rowIndex, y.columnIndex) } map 
                        { (left, right) => left :& right }
                        result.setName("MM: Logical And")
                        result
                      }
                      case Or => {
                        val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                        { y => (y.rowIndex, y.columnIndex) } map 
                        { (left, right) => left :| right }
                        result.setName("MM: Logical Or")
                        result
                      }
                    }
                  }
                  })
            }
            case operation => {
               handle[CellwiseMatrixMatrixTransformation, (Matrix, Matrix)](
            executable,
            { exec => (evaluate[Matrix](exec.left), evaluate[Matrix](exec.right)) },
            {
              case (_ , (left, right)) => {
                operation match {
                  case Addition => {
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) =>
                        left + right
                      }
                    result.setName("MM: Addition")
                    result
                  }
                  case Subtraction => {
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) => left - right }
                    result.setName("MM: Subtraction")
                    result
                  }
                  case Multiplication => {
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) => left :* right
                      }
                    result.setName("MM: Cellwise multiplication")
                    result
                  }
                  case Division => {
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) => left / right
                      }
                    result.setName("MM: Division")
                    result
                  }
                  case Maximum => {
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) =>
                       numerics.max(left, right)
                      }
                    result.setName("MM: Maximum")
                    result
                  }
                  case Minimum => {
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) =>
                        numerics.min(left, right)
                      }
                    result.setName("MM: Minimum")
                    result
                  }
                  case GreaterThan => {
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                    { y => (y.rowIndex, y.columnIndex) } map 
                    { (left,right) => left :> right }
                    result.setName("MM: Greater than")
                    result
                  }
                  case GreaterEqualThan => {
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                    {y => (y.rowIndex, y.columnIndex) } map 
                    { (left, right) => left :>= right }
                    result.setName("MM: Greater equal than")
                    result
                  }
                  case LessThan => {
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                    { y => (y.rowIndex, y.columnIndex) } map
                    { (left, right) => left :< right }
                    result.setName("MM: Less than")
                    result
                  }
                  case LessEqualThan => {
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                    { y => (y.rowIndex, y.columnIndex) } map
                    { (left, right) => left :<= right }
                    result.setName("MM: Less equal than")
                    result
                  }
                  case Equals =>
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                    { y => (y.rowIndex, y.columnIndex) } map
                    { (left, right) => left :== right }
                    result.setName("MM: Equals")
                    result
                  case NotEquals => { 
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                    { y => (y.rowIndex, y.columnIndex) } map
                    { (left, right) => left :!= right }
                    result.setName("MM: NotEquals")
                    result
                  }
                }
              }
            })
            }
          }
         
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
              val pathLiteral = path.getValue[StringValue](0,0)
              val source = DataSource("file://" + pathLiteral, CsvInputFormat[(Int, Int, Double)]("\n", ' '))

              val rowsCols = rows cross cols
              val rowsColsPair = rowsCols map { (rows, cols) => (rows.toInt, cols.toInt)}

              val blocks = rowsColsPair flatMap { case (rows, cols) =>
                val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows, cols)
                for (partition <- partitionPlan.iterator) yield {
                  (partition.id, partition)
                }
              }

              val partitionedData = rowsColsPair cross source map {
                case ((rows, cols), (row, column, value)) =>
                  val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows, cols)
                  (partitionPlan.partitionId(row-1, column-1), row-1, column-1, value)
              }

              partitionedData cogroup blocks where { entry => entry._1 } isEqualTo { block => block._1 } map {
                (entries, blocks) =>
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
        val executables = compound.executables flatMap { evaluate[List[ScalaSink[_]]](_) }
        new ScalaPlan(executables)
      }

      
      case executable: ones => {
        handle[ones, (Scalar[Double], Scalar[Double])](
          executable,
          { exec => (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numColumns)) },
          {
            case (_, (rows, columns)) => {
              val result = rows cross columns flatMap { (rows, columns) =>
                val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows.toInt, columns.toInt)

                for (matrixPartition <- partitionPlan.iterator) yield {
                  Submatrix.init(matrixPartition, 1.0)
                }
              }
              
              result.setName("Ones")
              result
            }
          })
      }
      
      case executable: zeros => {
        handle[zeros, (Scalar[Double], Scalar[Double])](
            executable,
            { exec => (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numCols))},
            { case (_, (rows, columns)) => {
              val result = rows cross columns flatMap { (rows, columns) => 
                val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows.toInt, columns.toInt)
                
                for(matrixPartition <- partitionPlan.iterator) yield {
                  Submatrix(matrixPartition)
                }
              }
              
              result.setName(s"Zeros")
              result
            }})
      }
      
      case executable: eye => {
        handle[eye, (Scalar[Double], Scalar[Double])](
            executable,
            { exec => (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numCols))},
            { case (_, (rows, columns)) => {
              val result = rows cross columns flatMap { (rows, columns) => 
                val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows.toInt, columns.toInt)
                
                for(matrixPartition <- partitionPlan.iterator) yield {
                  if(matrixPartition.rowIndex == matrixPartition.columnIndex){
                   Submatrix.eye(matrixPartition)
                  }else{
                    Submatrix(matrixPartition)
                  }
                }
              }
              result.setName("Eye")
              result
            }})
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
              val rowsCols = rows cross cols map { (rows, cols) => (rows, cols) } 
              rowsCols.setName("Randn: Rows and cols combined")
              
              val rowsColsMean = rowsCols cross mean map { case ((rows, cols), mean) => (rows, cols, mean) } 
              rowsColsMean.setName("Randn: Rows, cols and mean combined")
              
              val randomPartitions = rowsColsMean cross std flatMap
                {
                  case ((rows, cols, mean), std) =>
                    val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows.toInt, cols.toInt)

                    val result = for (partition <- partitionPlan.iterator) yield {
                      Submatrix.rand(partition, new GaussianRandom(mean, std))
                    }
                    
                    result
                }
              
              randomPartitions.setName("Randn: Random submatrices")
              randomPartitions
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
              (exec.matrix.rows, exec.matrix.cols) match {
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
        handle[FixpointIteration, (Matrix, Scalar[Double])](
          executable,
          { exec => (evaluate[Matrix](exec.initialState), evaluate[Scalar[Double]](exec.maxIterations)) },
          { case (exec, (initialState, maxIterations)) =>
            val numberIterations = maxIterations.getValue[DoubleValue](0,0).getValue.toInt
            def stepFunction(partialSolution: Matrix) = {
              val oldStatePlaceholderValue = iterationStatePlaceholderValue
              iterationStatePlaceholderValue = Some(partialSolution)
              val result = evaluate[Matrix](exec.updatePlan)
              iterationStatePlaceholderValue = oldStatePlaceholderValue
              /* 
               * Iteration mechanism requires that there is some kind of operation in the step function.
               * Therefore it is not possible to use the identity function!!! A workaround for this situation
               * would be to apply an explicit mapping operation with the identity function.
              */
              result
            }

            val terminationFunction = (prev: Matrix, cur: Matrix) => {
              val oldPreviousState = convergencePreviousStateValue
              val oldCurrentState = convergenceCurrentStateValue
              convergencePreviousStateValue = Some(prev)
              convergenceCurrentStateValue = Some(cur)
              val appliedConvergence = exec.convergence.apply(ConvergencePreviousStatePlaceholder,
  ConvergenceCurrentStatePlaceholder)
              val result = evaluate[Scalar[Boolean]](appliedConvergence)

              convergencePreviousStateValue = oldPreviousState
              convergenceCurrentStateValue = oldCurrentState
              result filter {
                boolean =>
                  boolean == false
              }
            }

            val iteration = initialState.iterateWithTermination(numberIterations, stepFunction, terminationFunction)
            iteration.setName("Fixpoint iteration")
            iteration
          })
      }

      case IterationStatePlaceholder => {
        iterationStatePlaceholderValue match {
          case Some(value) => value
          case None => throw new StratosphereExecutionError("The iteration state placeholder value was not set yet.")
        }
      }

      case executable: FixpointIterationCellArray => {
        handle[FixpointIterationCellArray, (CellArray, Scalar[Double])](
        executable,
        { exec => (evaluate[CellArray](exec.initialState), evaluate[Scalar[Double]](exec.maxIterations)) },
        { case (exec, (initialState, maxIterations)) =>
          val numberIterations = maxIterations.getValue[DoubleValue](0,0).getValue.toInt
          def stepFunction(partialSolution: CellArray) = {
            val oldStatePlaceholderValue = iterationStatePlaceholderValueCellArray
            iterationStatePlaceholderValueCellArray = Some(partialSolution)
            val result = evaluate[CellArray](exec.updatePlan)
            iterationStatePlaceholderValueCellArray = oldStatePlaceholderValue
            /*
             * Iteration mechanism requires that there is some kind of operation in the step function.
             * Therefore it is not possible to use the identity function!!! A workaround for this situation
             * would be to apply an explicit mapping operation with the identity function.
            */
            result
          }

          val terminationFunction = (prev: CellArray, cur: CellArray) => {
            val oldPreviousState = convergencePreviousStateCellArrayValue
            val oldCurrentState = convergenceCurrentStateCellArrayValue
            convergencePreviousStateCellArrayValue = Some(prev)
            convergenceCurrentStateCellArrayValue = Some(cur)

            val appliedConvergence = exec.convergence.apply(ConvergencePreviousStateCellArrayPlaceholder(exec.getType),
            ConvergenceCurrentStateCellArrayPlaceholder(exec.getType))
            val result = evaluate[Scalar[Boolean]](appliedConvergence)

            convergencePreviousStateCellArrayValue = oldPreviousState
            convergenceCurrentStateCellArrayValue = oldCurrentState

            result filter { boolean => boolean == false }
          }

          val iteration = initialState.iterateWithTermination(numberIterations, stepFunction, terminationFunction)
          iteration.setName("Fixpoint iteration")
          iteration
        })
      }

      case _:IterationStatePlaceholderCellArray => {
        iterationStatePlaceholderValueCellArray match {
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

      case executable: norm => {
        handle[norm, (Matrix, Scalar[Double])](
        executable,
        { exec => (evaluate[Matrix](exec.matrix), evaluate[Scalar[Double]](exec.p))},
        { case (_, (matrix, p)) =>
          val exponentiation = matrix cross p map {
            (matrix, p) =>
              matrix :^ p
          }
          exponentiation.setName("Norm: Exponentiation")
          val sums = exponentiation map {
            (matrix) =>
              matrix.activeValuesIterator.fold(0.0)( math.abs(_) + math.abs(_)
          )}
          sums.setName("Norm: Partial sums of submatrices")
          val totalSum = sums.combinableReduceAll{
            it =>
              it.fold(0.0)(_ + _)
          }
          val result = totalSum cross p map {
            (sum,p) =>
              math.pow(sum,1/p)
          }
          result.setName("Norm: Sum of partial sums")
          result
        }
        )
      }

      case executable: CellArrayExecutable => {
        handle[CellArrayExecutable, (List[Any])](
        executable,
        { exec => evaluate[Any](exec.elements)},
        { case (exec, elements) =>
          if(elements.length == 0){
            throw new StratosphereExecutionError("Cell arrays cannot be empty.")
          }else{
            val cellArrayEntries:List[DataSet[CellEntry]] = elements.zipWithIndex.map {
              case (element, index) =>
                exec.elements(index).getType match {
                  case DoubleType =>
                    element.asInstanceOf[Scalar[Double]] map { entry =>
                      CellEntry(index, ValueWrapper(entry))}
                  case BooleanType =>
                    element.asInstanceOf[Scalar[Boolean]] map {
                      entry => CellEntry(index, ValueWrapper(entry))
                    }
                  case StringType =>
                    element.asInstanceOf[Scalar[String]] map {
                      entry => CellEntry(index, ValueWrapper(entry))
                    }
                  case MatrixType(DoubleType) =>
                    element.asInstanceOf[Matrix] map {
                      entry => CellEntry(index, ValueWrapper(entry))
                    }
                  case MatrixType(BooleanType) =>
                    element.asInstanceOf[BooleanMatrix] map {
                      entry => CellEntry(index, ValueWrapper(entry))
                    }
                  case CellArrayType(_) =>
                    element.asInstanceOf[CellArray] map {
                      entry => CellEntry(index, ValueWrapper(entry))
                    }
                  case Undefined | Void | FunctionType | Unknown |
                    MatrixType(_) =>
                    throw new StratosphereExecutionError("Cannot create cell array from given type.")
                }
            }

            val firstEntry = cellArrayEntries.head
            val result = cellArrayEntries.tail.foldLeft(firstEntry)(_ union _)

            result
          }
        }
        )
      }

      case executable: CellArrayReferenceString => {
        handle[CellArrayReferenceString, CellArray](
        executable,
        { exec => evaluate[CellArray](exec.parent)},
        { (exec, cellArray) =>
          cellArray filter { x => x.index == exec.reference } map { x => x.wrappedValue[String] }
        }
        )
      }

      case executable: CellArrayReferenceScalar => {
        handle[CellArrayReferenceScalar, CellArray](
        executable,
        {exec => evaluate[CellArray](exec.parent)},
        { (exec, cellArray) =>
          exec.getType match {
            case DoubleType =>
              cellArray filter { x => x.index == exec.reference } map { x => x.wrappedValue[Double]}
            case BooleanType =>
              cellArray filter { x => x.index == exec.reference } map { x => x.wrappedValue[Boolean]}
          }

        }
        )
      }

      case executable: CellArrayReferenceMatrix => {
        handle[CellArrayReferenceMatrix, CellArray](
        executable,
        {exec => evaluate[CellArray](exec.parent)},
        {(exec, cellArray) =>
          val filtered = cellArray filter { x => x.index == exec.reference }
          val tpe = exec.getType
          tpe match {
            case MatrixType(DoubleType) =>
              filtered map {
                x => x.wrappedValue[Submatrix]}
            case MatrixType(BooleanType) =>
              filtered map { x => x.wrappedValue[SubmatrixBoolean]}
          }
        }
        )
      }

      case executable: CellArrayReferenceCellArray => {
        handle[CellArrayReferenceCellArray, CellArray](
        executable,
        {exec => evaluate[CellArray](exec.parent)},
        {(exec, cellArray) =>
          cellArray filter { x => x.index == exec.reference } map { x => x.wrappedValue[CellEntry]}
        }
        )
      }

      case executable: CellArrayReferenceFunction => {
        throw new StratosphereExecutionError("Cannot execute function. Needs function application")
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

      case ConvergencePreviousStatePlaceholder => {
        convergencePreviousStateValue match {
          case Some(matrix) => matrix
          case None => throw new StratosphereExecutionError("Convergence previous state value has not been set.")
        }
      }

      case ConvergenceCurrentStatePlaceholder => {
        convergenceCurrentStateValue match {
          case Some(matrix) => matrix
          case None => throw new StratosphereExecutionError("Convergence current state value has not been set.")
        }
      }

      case placeholder: ConvergenceCurrentStateCellArrayPlaceholder => {
        convergenceCurrentStateCellArrayValue match {
          case Some(cellArray) => cellArray
          case None => throw new StratosphereExecutionError("Convergence current state cell array value has not been " +
            "set.")
        }
      }

      case placeholder: ConvergencePreviousStateCellArrayPlaceholder => {
        convergencePreviousStateCellArrayValue match {
          case Some(cellArray) => cellArray
          case None => throw new StratosphereExecutionError("Convergence previous state cell array value has not been" +
            " set.")
        }
      }


    }
  }
}