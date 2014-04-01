package org.gilbertlang.runtime.execution.stratosphere

import org.gilbertlang.runtime.Executor
import eu.stratosphere.api.scala.operators.{CsvInputFormat, CsvOutputFormat, DelimitedOutputFormat}
import eu.stratosphere.api.scala._
import scala.collection.convert.WrapAsScala
import org.gilbertlang.runtime.Operations._
import org.gilbertlang.runtime.Executables._
import org.gilbertlang.runtimeMacros.linalg._
import org.gilbertlang.runtime.execution.CellwiseFunctions
import breeze.linalg.norm
import breeze.linalg.*
import scala.language.implicitConversions
import scala.language.reflectiveCalls
import org.gilbertlang.runtime.RuntimeTypes._
import eu.stratosphere.api.scala.CollectionDataSource
import eu.stratosphere.types.{DoubleValue, StringValue}
import org.gilbertlang.runtime.Executables.diag
import org.gilbertlang.runtimeMacros.linalg.numerics
import org.gilbertlang.runtime.Executables.VectorwiseMatrixTransformation
import org.gilbertlang.runtime.Executables.WriteMatrix
import org.gilbertlang.runtime.Executables.MatrixParameter
import scala.Some
import org.gilbertlang.runtime.Executables.ConvergenceCurrentStateCellArrayPlaceholder
import org.gilbertlang.runtime.Executables.eye
import org.gilbertlang.runtime.Executables.TypeConversionMatrix
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
import org.gilbertlang.runtime.Executables.TypeConversionScalar
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
import org.gilbertlang.runtime.Executables.repmat
import org.gilbertlang.runtime.Executables.Transpose
import org.gilbertlang.runtime.Executables.ConvergencePreviousStateCellArrayPlaceholder
import org.gilbertlang.runtimeMacros.linalg.Partition
import org.gilbertlang.runtime.RuntimeTypes.MatrixType
import org.gilbertlang.runtime.Executables.function
import org.gilbertlang.runtime.Executables.sumRow
import org.gilbertlang.runtime.Executables.WriteString
import scala.language.postfixOps


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

      case executable: WriteMatrix =>
        executable.matrix.getType match {
          case MatrixType(DoubleType,_,_) =>
            handle[WriteMatrix, Matrix](
            executable,
            { exec => evaluate[Matrix](exec.matrix) },
            { (_, matrix) =>
            {
              val completePathWithFilename = newTempFileName()
              List(matrix.write(completePathWithFilename, DelimitedOutputFormat(Submatrix.outputFormatter("\n", " "),
                ""), s"WriteMatrix($completePathWithFilename)"))
            }
            })
          case MatrixType(BooleanType,_,_) =>
            handle[WriteMatrix, BooleanMatrix](
            executable,
            { exec => evaluate[BooleanMatrix](exec.matrix) },
            { (_, matrix) =>
            {
              val completePathWithFilename = newTempFileName()
              List(matrix.write(completePathWithFilename, DelimitedOutputFormat(SubmatrixBoolean.outputFormatter("\n", " "),
                ""), s"WriteMatrix($completePathWithFilename)"))
            }
            })
        }

      case executable: WriteCellArray =>
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
            val filtered = cellArray filter {
              x =>
                x.index == loopIndex
            }

            filtered.setName("WriteCellArray: Select entry")
            val sink = cellArrayType.elementTypes(index) match {
              case MatrixType(DoubleType,_,_) =>
                val mappedCell = filtered map {
                  x =>
                    x.wrappedValue[Submatrix]
                }
                mappedCell.setName("WriteCellArray: Unwrapped value Matrix(Double)")
                mappedCell.write(completePathWithFilename, DelimitedOutputFormat(Submatrix.outputFormatter("\n", " "),
                  ""),
                  s"WriteCellArray(Matrix[Double], $completePathWithFilename)")
              case StringType =>
                val mappedCell =filtered map( x => x.wrappedValue[String])
                mappedCell.setName("WriteCellArray: Unwrapped value String")
                mappedCell.write(completePathWithFilename, CsvOutputFormat(),
                  s"WriteCellArray(String, $completePathWithFilename)")
              case DoubleType =>
                val mappedCell = filtered map(x => x.wrappedValue[Double])
                mappedCell.setName("WriteCellArray: Unwrapped value Double")
                mappedCell.write(completePathWithFilename, CsvOutputFormat(), s"WriteCellArray(Double," +
                  s"$completePathWithFilename)")
              case BooleanType =>
                val mappedCell = filtered map(x => x.wrappedValue[Boolean])
                mappedCell.setName("WriteCellArray: Unwrapped value Boolean")
                mappedCell.write(completePathWithFilename, CsvOutputFormat(), s"WriteCellArray(Boolean," +
                  s"$completePathWithFilename)")
            }
            result(index) = sink
            index += 1
          }

          result.toList
        }
        )

      case executable: WriteString =>
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

      //TODO: Fix
      case executable: WriteFunction =>
        throw new TransformationNotSupportedError("WriteFunction is not supported by Stratosphere")

      //TODO: Fix
      case executable: WriteScalar =>
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

      case VoidExecutable =>
        null

      case executable: ScalarMatrixTransformation =>
        executable.operation match {
          case logicOperation: LogicOperation =>
            handle[ScalarMatrixTransformation, (Scalar[Boolean], BooleanMatrix)](
                executable,
                { exec => (evaluate[Scalar[Boolean]](exec.scalar), evaluate[BooleanMatrix](exec.matrix))},
                { case (_, (scalar, matrix)) =>
                  logicOperation match {
                    case And =>
                      val result = scalar cross matrix map { (scalar, submatrix) => submatrix :& scalar }
                      result.setName("SM: Logical And")
                      result
                    case Or =>
                      val result = scalar cross matrix map { (scalar, submatrix) => submatrix :| scalar }
                      result.setName("SM: Logical Or")
                      result
                  }
                })
          case operation =>
            handle[ScalarMatrixTransformation, (Scalar[Double], Matrix)](
          executable,
          { exec => (evaluate[Scalar[Double]](exec.scalar), evaluate[Matrix](exec.matrix)) },
          {
            case (_, (scalar, matrix)) =>
              operation match {
                case Addition =>
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix + scalar }
                  result.setName("SM: Addition")
                  result
                case Subtraction =>
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix + -scalar }
                  result.setName("SM: Subtraction")
                  result
                case Multiplication =>
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix * scalar }
                  result.setName("SM: Multiplication")
                  result
                case Division =>
                  val result = scalar cross matrix map { (scalar, submatrix) =>
                    {
                      val partition = submatrix.getPartition
                      val result = Submatrix.init(partition, scalar)
                      result / submatrix
                    }
                  }
                  result.setName("SM: Division")
                  result
                case GreaterThan =>
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix :< scalar }
                  result.setName("SM: Greater than")
                  result
                case GreaterEqualThan =>
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix :<= scalar }
                  result.setName("SM: Greater equal than")
                  result
                case LessThan =>
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix :> scalar }
                  result.setName("SM: Less than")
                  result
                case LessEqualThan =>
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix :>= scalar }
                  result.setName("SM: Less equal than")
                  result
                case Equals =>
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix :== scalar }
                  result.setName("SM: Equals")
                  result
                case NotEquals =>
                  val result = scalar cross matrix map { (scalar, submatrix) => submatrix :!= scalar }
                  result.setName("SM: Not equals")
                  result
              }
          })
        }

      case executable: MatrixScalarTransformation =>
        executable.operation match {
          case logicOperation: LogicOperation =>
            handle[MatrixScalarTransformation, (BooleanMatrix, Scalar[Boolean])](
                executable,
                {exec => (evaluate[BooleanMatrix](exec.matrix), evaluate[Scalar[Boolean]](exec.scalar))},
                { case (_, (matrix, scalar)) =>
                  logicOperation match {
                    case And =>
                      val result = matrix cross scalar map { (submatrix, scalar) => submatrix :& scalar }
                      result.setName("MS: Logical And")
                      result
                    case Or =>
                      val result = matrix cross scalar map { (submatrix, scalar) => submatrix :| scalar }
                      result.setName("MS: Logical Or")
                      result
                  }
                })
          case operation =>
            handle[MatrixScalarTransformation, (Matrix, Scalar[Double])](
          executable,
          { exec => (evaluate[Matrix](exec.matrix), evaluate[Scalar[Double]](exec.scalar)) },
          {
            case (_, (matrix, scalar)) =>
              operation match {
                case Addition =>
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix + scalar }
                  result.setName("MS: Addition")
                  result
                case Subtraction =>
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix - scalar }
                  result.setName("MS: Subtraction")
                  result
                case Multiplication =>
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix * scalar }
                  result.setName("MS: Multiplication")
                  result
                case Division =>
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix / scalar }
                  result.setName("MS: Division")
                  result
                case GreaterThan =>
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix :> scalar }
                  result.setName("MS: Greater than")
                  result
                case GreaterEqualThan =>
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix :>= scalar }
                  result.setName("MS: Greater equal than")
                  result
                case LessThan =>
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix :< scalar }
                  result.setName("MS: Less than")
                  result
                case LessEqualThan =>
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix :<= scalar }
                  result.setName("MS: Less equal than")
                  result
                case Equals =>
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix :== scalar }
                  result.setName("MS: Equals")
                  result
                case NotEquals =>
                  val result = matrix cross scalar map { (submatrix, scalar) => submatrix :!= scalar }
                  result.setName("MS: Not equals")
                  result
              }
          })
        }

      case executable: ScalarScalarTransformation =>

        executable.operation match {
          case logicOperation: LogicOperation =>
            handle[ScalarScalarTransformation, (Scalar[Boolean], Scalar[Boolean])](
              executable,
              {exec => (evaluate[Scalar[Boolean]](exec.left), evaluate[Scalar[Boolean]](exec.right))},
              {case (_, (left, right)) =>
                logicOperation match {
                  case And =>
                    val result = left cross right map { (left, right) => left && right }
                    result.setName("SS: Logical And")
                    result
                  case Or =>
                    val result = left cross right map { (left, right) => left || right }
                    result.setName("SS: Logical Or")
                    result
                }
              }
            )
          case operation =>
            handle[ScalarScalarTransformation, (Scalar[Double], Scalar[Double])](
              executable,
              { exec => (evaluate[Scalar[Double]](exec.left), evaluate[Scalar[Double]](exec.right)) },
              {
                case (_, (left, right)) =>
                  operation match {
                    case Addition =>
                      val result = left cross right map { (left, right) => left + right }
                      result.setName("SS: Addition")
                      result
                    case Subtraction =>
                      val result = left cross right map { (left, right) => left - right }
                      result.setName("SS: Subtraction")
                      result
                    case Multiplication =>
                      val result = left cross right map { (left, right) => left * right }
                      result.setName("SS: Multiplication")
                      result
                    case Division =>
                      val result =left cross right map { (left, right) => left / right }
                      result.setName("SS: Division")
                      result
                    case Maximum =>
                      val result = left union right combinableReduceAll { elements => elements.max }
                      result.setName("SS: Maximum")
                      result
                    case Minimum =>
                      val result = left union right combinableReduceAll { elements => elements.min }
                      result.setName("SS: Minimum")
                      result
                    case GreaterThan =>
                      val result = left cross right map { (left, right) => left > right }
                      result.setName("SS: Greater than")
                      result
                    case GreaterEqualThan =>
                      val result = left cross right map { (left, right) => left >= right }
                      result.setName("SS: Greater equal than")
                      result
                    case LessThan =>
                      val result = left cross right map { (left, right) => left < right }
                      result.setName("SS: Less than")
                      result
                    case LessEqualThan =>
                      val result = left cross right map { (left, right) => left <= right }
                      result.setName("SS: Less equal than")
                      result
                    case Equals =>
                      val result = left cross right map { (left, right) => left == right}
                      result.setName("SS: Equals")
                      result
                    case NotEquals =>
                      val result = left cross right map { (left, right) => left != right }
                      result.setName("SS: Not equals")
                      result
                  }
              })
        }

      case executable: AggregateMatrixTransformation =>
        handle[AggregateMatrixTransformation, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (exec, matrix) =>
            {
              exec.operation match {
                case Maximum =>
                  matrix map { x => x.max } combinableReduceAll
                    { elements => elements.max }
                case Minimum =>
                  matrix map { x => x.min } combinableReduceAll
                    { elements => elements.min }
                case Norm2 =>
                  matrix map { x => breeze.linalg.sum(x:*x) } combinableReduceAll
                    { x => x.fold(0.0)(_ + _) } map
                    { x => math.sqrt(x) }
              }
            }
          })

      case executable: UnaryScalarTransformation =>
        handle[UnaryScalarTransformation, Scalar[Double]](
          executable,
          { exec => evaluate[Scalar[Double]](exec.scalar) },
          { (exec, scalar) =>
            {
              exec.operation match {
                case Minus =>
                  scalar map { x => -x }
                case Binarize =>
                  scalar map { x => CellwiseFunctions.binarize(x) }
              }
            }
          })

      case executable: scalar =>
        handle[scalar, Unit](
          executable,
          { _ => },
          { (exec, _) => CollectionDataSource(List(exec.value)) })

      case executable: boolean =>
        handle[boolean, Unit](
            executable,
            {_ => },
            { (exec, _) => CollectionDataSource(List(exec.value)) })

      case executable: string =>
        handle[string, Unit](
          executable,
          { _ => },
          { (exec, _) => CollectionDataSource(List(exec.value))})

      case executable: CellwiseMatrixTransformation =>
        handle[CellwiseMatrixTransformation, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (exec, matrix) =>
            {
              exec.operation match {
                case Minus =>
                  matrix map { submatrix => submatrix * -1.0}
                case Binarize =>
                  matrix map { submatrix =>
                    {
                      submatrix.mapActiveValues(x => CellwiseFunctions.binarize(x))
                    }
                  }
              }
            }
          })

      case executable: CellwiseMatrixMatrixTransformation =>
        executable.operation match {
          case logicOperation: LogicOperation =>
            handle[CellwiseMatrixMatrixTransformation, (BooleanMatrix, BooleanMatrix)](
                executable,
                { exec => (evaluate[BooleanMatrix](exec.left), evaluate[BooleanMatrix](exec.right))},
                { case (_, (left, right)) =>
                  logicOperation match {
                    case And =>
                      val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) => left :& right }
                      result.setName("MM: Logical And")
                      result
                    case Or =>
                      val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) => left :| right }
                      result.setName("MM: Logical Or")
                      result
                  }
                })
          case operation =>
            handle[CellwiseMatrixMatrixTransformation, (Matrix, Matrix)](
          executable,
          { exec => (evaluate[Matrix](exec.left), evaluate[Matrix](exec.right)) },
          {
            case (_ , (left, right)) =>
              operation match {
                case Addition =>
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                    { y => (y.rowIndex, y.columnIndex) } map
                    { (left, right) =>
                      left + right
                    }
                  result.setName("MM: Addition")
                  result
                case Subtraction =>
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                    { y => (y.rowIndex, y.columnIndex) } map
                    { (left, right) => left - right }
                  result.setName("MM: Subtraction")
                  result
                case Multiplication =>
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                    { y => (y.rowIndex, y.columnIndex) } map
                    { (left, right) => left :* right
                    }
                  result.setName("MM: Cellwise multiplication")
                  result
                case Division =>
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                    { y => (y.rowIndex, y.columnIndex) } map
                    { (left, right) => left / right
                    }
                  result.setName("MM: Division")
                  result
                case Maximum =>
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                    { y => (y.rowIndex, y.columnIndex) } map
                    { (left, right) =>
                     numerics.max(left, right)
                    }
                  result.setName("MM: Maximum")
                  result
                case Minimum =>
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                    { y => (y.rowIndex, y.columnIndex) } map
                    { (left, right) =>
                      numerics.min(left, right)
                    }
                  result.setName("MM: Minimum")
                  result
                case GreaterThan =>
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                  { y => (y.rowIndex, y.columnIndex) } map
                  { (left,right) => left :> right }
                  result.setName("MM: Greater than")
                  result
                case GreaterEqualThan =>
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                  {y => (y.rowIndex, y.columnIndex) } map
                  { (left, right) => left :>= right }
                  result.setName("MM: Greater equal than")
                  result
                case LessThan =>
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                  { y => (y.rowIndex, y.columnIndex) } map
                  { (left, right) => left :< right }
                  result.setName("MM: Less than")
                  result
                case LessEqualThan =>
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                  { y => (y.rowIndex, y.columnIndex) } map
                  { (left, right) => left :<= right }
                  result.setName("MM: Less equal than")
                  result
                case Equals =>
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                  { y => (y.rowIndex, y.columnIndex) } map
                  { (left, right) => left :== right }
                  result.setName("MM: Equals")
                  result
                case NotEquals =>
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                  { y => (y.rowIndex, y.columnIndex) } map
                  { (left, right) => left :!= right }
                  result.setName("MM: NotEquals")
                  result
              }
          })
        }

      case executable: MatrixMult =>
        handle[MatrixMult, (Matrix, Matrix)](
          executable,
          { exec => (evaluate[Matrix](exec.left), evaluate[Matrix](exec.right)) },
          {
            case (_, (left, right)) =>
              left join right where { leftElement => leftElement.columnIndex } isEqualTo
                { rightElement => rightElement.rowIndex } map
                { (left, right) => left * right } groupBy
                { element => (element.rowIndex, element.columnIndex) } combinableReduceGroup
                { elements =>
                  {
                    val element = elements.next().copy
                    elements.foldLeft(element)({ _ + _ })
                  }
                }
          })

      case executable: Transpose =>
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

      case executable: VectorwiseMatrixTransformation =>
        handle[VectorwiseMatrixTransformation, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (exec, matrix) =>
            {
              exec.operation match {
                case NormalizeL1 =>
                  matrix map { submatrix =>
                    norm(submatrix( * , ::),1)
                  } groupBy (subvector => subvector.index) combinableReduceGroup {
                    subvectors =>
                      {
                        val firstElement = subvectors.next().copy
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
                case Maximum =>
                  matrix map { submatrix => numerics.max(submatrix(*, ::)) } groupBy
                    { subvector => subvector.index } combinableReduceGroup { subvectors =>
                      val firstElement = subvectors.next().copy
                      subvectors.foldLeft(firstElement) { numerics.max(_, _) }
                    } map { subvector => subvector.asMatrix }
                case Minimum =>
                  matrix map { submatrix => numerics.min(submatrix(*, ::)) } groupBy
                    { subvector => subvector.index } combinableReduceGroup { subvectors =>
                      val firstElement = subvectors.next().copy
                      subvectors.foldLeft(firstElement) { numerics.min(_, _) }
                    } map { subvector => subvector.asMatrix }
                case Norm2 =>
                  val squaredValues = matrix map { submatrix => submatrix :^ 2.0 }
                  squaredValues.setName("VWM: Norm2 squared values")

                  val sumSquaredValues = squaredValues map { submatrix => breeze.linalg.sum(submatrix(*, ::)) } groupBy
                    { subvector => subvector.index } combinableReduceGroup
                    { subvectors =>
                      val firstSubvector = subvectors.next().copy
                      subvectors.foldLeft(firstSubvector)(_ + _)
                    }
                  sumSquaredValues.setName("VWM: Norm2 sum squared values")

                  val result = sumSquaredValues map {
                    sqv =>
                      val matrix = sqv.asMatrix
                      matrix mapActiveValues { value => math.sqrt(value) }
                  }
                  result.setName("VWM: Norm2")
                  result
              }
            }
          })

      case executable: LoadMatrix =>
        handle[LoadMatrix, (Scalar[String], Scalar[Double], Scalar[Double])](
          executable,
          { exec =>
            (evaluate[Scalar[String]](exec.path), evaluate[Scalar[Double]](exec.numRows),
            evaluate[Scalar[Double]](exec.numColumns))
          },
          {
            case (_, (path, rows, cols)) =>
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

                val partition = blocks.next()._2

                if (blocks.hasNext) {
                  throw new IllegalArgumentError("LoadMatrix coGroup phase must have at most one block")
                }


                Submatrix(partition, (entries map { case (id, row, col, value) => (row, col, value)}).toSeq)
              }
          })

      case compound: CompoundExecutable =>
        val executables = compound.executables flatMap { evaluate[List[ScalaSink[_]]] }
        new ScalaPlan(executables)


      case executable: ones =>
        handle[ones, (Scalar[Double], Scalar[Double])](
          executable,
          { exec => (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numColumns)) },
          {
            case (_, (rows, columns)) =>
              val result = rows cross columns flatMap { (rows, columns) =>
                val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows.toInt, columns.toInt)

                for (matrixPartition <- partitionPlan.iterator) yield {
                  Submatrix.init(matrixPartition, 1.0)
                }
              }

              result.setName("Ones")
              result
          })

      case executable: zeros =>
        handle[zeros, (Scalar[Double], Scalar[Double])](
            executable,
            { exec => (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numCols))},
            { case (_, (rows, columns)) =>
              val result = rows cross columns flatMap { (rows, columns) =>
                val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows.toInt, columns.toInt)

                for(matrixPartition <- partitionPlan.iterator) yield {
                  Submatrix(matrixPartition)
                }
              }

              result.setName(s"Zeros")
              result
            })

      case executable: eye =>
        handle[eye, (Scalar[Double], Scalar[Double])](
            executable,
            { exec => (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numCols))},
            { case (_, (rows, columns)) =>
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
            })

      case executable: randn =>
        handle[randn, (Scalar[Double], Scalar[Double], Scalar[Double], Scalar[Double])](
          executable,
          { exec =>
            (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numColumns),
              evaluate[Scalar[Double]](exec.mean), evaluate[Scalar[Double]](exec.std))
          },
          {
            case (_, (rows, cols, mean, std)) =>
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
          })

      case executable: spones =>
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

      case executable: sumRow =>
        handle[sumRow, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (_, matrix) =>
            {
              matrix map { submatrix => breeze.linalg.sum(submatrix(*, ::)) } groupBy
                { subvector => subvector.index } combinableReduceGroup
                { subvectors =>
                  val firstSubvector = subvectors.next().copy
                  subvectors.foldLeft(firstSubvector)(_ + _)
                } map
                { subvector => subvector.asMatrix }
            }
          })

      case executable: sumCol =>
        handle[sumCol, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (_, matrix) =>
            {
              matrix map { submatrix => breeze.linalg.sum(submatrix(::, *)) } groupBy
                { submatrix => submatrix.columnIndex } combinableReduceGroup
                { subvectors =>
                  val firstSubvector = subvectors.next().copy
                  subvectors.foldLeft(firstSubvector)(_ + _)
                }
            }
          })

      case executable: diag =>
        handle[diag, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (exec, matrix) =>
            {
              (exec.matrix.rows, exec.matrix.cols) match {
                case (Some(1), _) =>
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
                case (_, Some(1)) =>
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
                case _ =>
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
                      val result = results.next().copy
                      results.foldLeft(result)(_ + _)
                  }
              }
            }
          })

      case executable: FixpointIteration =>
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

            var iteration: Matrix = null

            if(exec.convergence != null){
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

              iteration = initialState.iterateWithTermination(numberIterations, stepFunction, terminationFunction)
            }else{
              iteration = initialState.iterate(numberIterations, stepFunction)
            }

            iteration.setName("Fixpoint iteration")
            iteration
          })

      case IterationStatePlaceholder =>
        iterationStatePlaceholderValue match {
          case Some(value) => value
          case None => throw new StratosphereExecutionError("The iteration state placeholder value was not set yet.")
        }

      case executable: FixpointIterationCellArray =>
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

          var iteration: CellArray = null

          if(exec.convergence != null){
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

            iteration = initialState.iterateWithTermination(numberIterations, stepFunction, terminationFunction)
          }else{
            iteration = initialState.iterate(numberIterations, stepFunction)
          }

          iteration.setName("Fixpoint iteration")
          iteration
        })

      case _:IterationStatePlaceholderCellArray =>
        iterationStatePlaceholderValueCellArray match {
          case Some(value) => value
          case None => throw new StratosphereExecutionError("The iteration state placeholder value was not set yet.")
        }

      case executable: sum =>
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
                val firstSubvector = submatrices.next()
                (firstSubvector._1, submatrices.foldLeft(firstSubvector._2.copy)(_ + _._2))
              } map { case (_, submatrix) => submatrix}
          })

      case executable: norm =>
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

      case executable: CellArrayExecutable =>
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
                  case MatrixType(DoubleType,_,_) =>
                    element.asInstanceOf[Matrix] map {
                      entry => CellEntry(index, ValueWrapper(entry))
                    }
                  case MatrixType(BooleanType,_,_) =>
                    element.asInstanceOf[BooleanMatrix] map {
                      entry => CellEntry(index, ValueWrapper(entry))
                    }
                  case CellArrayType(_) =>
                    element.asInstanceOf[CellArray] map {
                      entry => CellEntry(index, ValueWrapper(entry))
                    }
                  case Undefined | Void | FunctionType | Unknown |
                    MatrixType(_,_,_) =>
                    throw new StratosphereExecutionError("Cannot create cell array from given type.")
                }
            }

            val firstEntry = cellArrayEntries.head
            val result = cellArrayEntries.tail.foldLeft(firstEntry)(_ union _)

            result
          }
        }
        )

      case executable: CellArrayReferenceString =>
        handle[CellArrayReferenceString, CellArray](
        executable,
        { exec => evaluate[CellArray](exec.parent)},
        { (exec, cellArray) =>
          cellArray filter { x => x.index == exec.reference } map { x => x.wrappedValue[String] }
        }
        )

      case executable: CellArrayReferenceScalar =>
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

      case executable: CellArrayReferenceMatrix =>
        handle[CellArrayReferenceMatrix, CellArray](
        executable,
        {exec => evaluate[CellArray](exec.parent)},
        {(exec, cellArray) =>
          val filtered = cellArray filter { x => x.index == exec.reference }
          val tpe = exec.getType
          tpe match {
            case MatrixType(DoubleType,_,_) =>
              filtered map {
                x => x.wrappedValue[Submatrix]}
            case MatrixType(BooleanType,_,_) =>
              filtered map { x => x.wrappedValue[SubmatrixBoolean]}
          }
        }
        )

      case executable: CellArrayReferenceCellArray =>
        handle[CellArrayReferenceCellArray, CellArray](
        executable,
        {exec => evaluate[CellArray](exec.parent)},
        {(exec, cellArray) =>
          cellArray filter { x => x.index == exec.reference } map { x => x.wrappedValue[CellEntry]}
        }
        )

      case function: function =>
        throw new StratosphereExecutionError("Cannot execute function. Needs function application")

      case parameter: StringParameter =>
        throw new StratosphereExecutionError("Parameter found. Cannot execute parameters.")

      case parameter: ScalarParameter =>
        throw new StratosphereExecutionError("Parameter found. Cannot execute parameters.")

      case parameter: MatrixParameter =>
        throw new StratosphereExecutionError("Parameter found. Cannot execute parameters.")

      case parameter: FunctionParameter =>
        throw new StratosphereExecutionError("Parameter found. Cannot execute parameters.")

      case ConvergencePreviousStatePlaceholder =>
        convergencePreviousStateValue match {
          case Some(matrix) => matrix
          case None => throw new StratosphereExecutionError("Convergence previous state value has not been set.")
        }

      case ConvergenceCurrentStatePlaceholder =>
        convergenceCurrentStateValue match {
          case Some(matrix) => matrix
          case None => throw new StratosphereExecutionError("Convergence current state value has not been set.")
        }

      case placeholder: ConvergenceCurrentStateCellArrayPlaceholder =>
        convergenceCurrentStateCellArrayValue match {
          case Some(cellArray) => cellArray
          case None => throw new StratosphereExecutionError("Convergence current state cell array value has not been " +
            "set.")
        }

      case placeholder: ConvergencePreviousStateCellArrayPlaceholder =>
        convergencePreviousStateCellArrayValue match {
          case Some(cellArray) => cellArray
          case None => throw new StratosphereExecutionError("Convergence previous state cell array value has not been" +
            " set.")
        }

      case typeConversion: TypeConversionScalar =>
        (typeConversion.sourceType, typeConversion.targetType) match {
          case (BooleanType, DoubleType) =>
            handle[TypeConversionScalar, Scalar[Boolean]](
            typeConversion,
            { input => evaluate[Scalar[Boolean]](input.scalar)},
            { (_, scalar) =>  scalar map { x => if(x == true) 1.0 else 0.0} }
            )
        }

      case typeConversion: TypeConversionMatrix =>
        (typeConversion.sourceType, typeConversion.targetType) match {
          case (MatrixType(BooleanType,_,_), MatrixType(DoubleType,_,_)) =>
            handle[TypeConversionMatrix, BooleanMatrix](
            typeConversion,
            {input => evaluate[BooleanMatrix](input.matrix)},
            {
              (_, matrix) => matrix map { submatrix =>
                Submatrix(submatrix.getPartition, submatrix.activeIterator map {
                  case ((row,col), value) =>
                    (row,col, if(value) 1.0 else 0.0)
                  } toSeq
                )
              }
            })
        }

      case r: repmat =>
        r.matrix.getType match {
          case MatrixType(DoubleType, _, _) =>
            handle[repmat, (Matrix, Scalar[Double], Scalar[Double])](
              r,
              {r => (evaluate[Matrix](r.matrix), evaluate[Scalar[Double]](r.numRows),
                evaluate[Scalar[Double]](r.numCols))},
              {
                case (_, (matrix, rowsMult, colsMult)) =>
                  val rowsColsMult = rowsMult cross colsMult map { (rowsMult, colsMult) => (rowsMult.toInt, colsMult.toInt)}

                  val newBlocks = matrix cross rowsColsMult flatMap { case (matrix, (rowsMult, colsMult)) =>
                    val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rowsMult*matrix.totalRows,
                      colsMult*matrix.totalColumns)
                    val oldPartitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, matrix.totalRows,
                      matrix.totalColumns)
                    val rowIncrementor = oldPartitionPlan.maxRowIndex
                    val colIncrementor = oldPartitionPlan.maxColumnIndex


                    val result = for(rowIdx <- matrix.rowIndex until partitionPlan.maxRowIndex by rowIncrementor;
                    colIdx <- matrix.columnIndex until partitionPlan.maxColumnIndex by colIncrementor) yield{
                      val partition = partitionPlan.getPartition(rowIdx, colIdx)
                      (partition.id, partition)
                    }

                    result.toIterator
                  }

                  newBlocks.setName("Repmat: New blocks")

                  val repmatEntries = matrix cross rowsColsMult flatMap { case (matrix, (rowsMult, colsMult)) =>
                    val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE,
                      rowsMult*matrix.totalRows, colsMult*matrix.totalColumns)

                    matrix.activeIterator flatMap { case ((row, col), value) =>
                      for(rMult <- 0 until rowsMult; cMult <- 0 until colsMult) yield {
                        (partitionPlan.partitionId(rMult*matrix.totalRows + row, cMult*matrix.totalColumns + col),
                          rMult*matrix.totalRows + row, cMult*matrix.totalColumns + col, value)
                      }
                    }
                  }
                  repmatEntries.setName("Repmat: Repeated entries")

                  val result = newBlocks cogroup repmatEntries where { block => block._1} isEqualTo { entry => entry
                    ._1} map {
                    (blocks, entries) =>
                      if(!blocks.hasNext){
                        throw new IllegalArgumentError("LoadMatrix coGroup phase must have at least one block")
                      }

                      val partition = blocks.next()._2

                      if (blocks.hasNext) {
                        throw new IllegalArgumentError("LoadMatrix coGroup phase must have at most one block")
                      }

                      Submatrix(partition, (entries map { case (id, row, col, value) => (row, col, value)}).toSeq)
                  }
                  result.setName("Repmat: Repeated matrix")
                  result

              }
            )
          case MatrixType(BooleanType, _, _) =>
            handle[repmat, (BooleanMatrix, Scalar[Double], Scalar[Double])](
            r,
            {r => (evaluate[BooleanMatrix](r.matrix), evaluate[Scalar[Double]](r.numRows),
              evaluate[Scalar[Double]](r.numCols))},
            {
              case (_, (matrix, rowsMult, colsMult)) =>
                val rowsColsMult = rowsMult cross colsMult map { (rowsMult, colsMult) => (rowsMult.toInt, colsMult.toInt)}

                val newBlocks = matrix cross rowsColsMult flatMap { case (matrix, (rowsMult, colsMult)) =>
                  val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rowsMult*matrix.totalRows,
                    colsMult*matrix.totalColumns)
                  val oldPartitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, matrix.totalRows,
                    matrix.totalColumns)
                  val rowIncrementor = oldPartitionPlan.maxRowIndex
                  val colIncrementor = oldPartitionPlan.maxColumnIndex


                  val result = for(rowIdx <- matrix.rowIndex until partitionPlan.maxRowIndex by rowIncrementor;
                                   colIdx <- matrix.columnIndex until partitionPlan.maxColumnIndex by colIncrementor) yield{
                    val partition = partitionPlan.getPartition(rowIdx, colIdx)
                    (partition.id, partition)
                  }

                  result.toIterator
                }

                newBlocks.setName("Repmat: New blocks")

                val repmatEntries = matrix cross rowsColsMult flatMap { case (matrix, (rowsMult, colsMult)) =>
                  val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE,
                    rowsMult*matrix.totalRows, colsMult*matrix.totalColumns)

                  matrix.activeIterator flatMap { case ((row, col), value) =>
                    for(rMult <- 0 until rowsMult; cMult <- 0 until colsMult) yield {
                      (partitionPlan.partitionId(rMult*matrix.totalRows + row, cMult*matrix.totalColumns + col),
                        rMult*matrix.totalRows + row, cMult*matrix.totalColumns + col, value)
                    }
                  }
                }
                repmatEntries.setName("Repmat: Repeated entries")

                val result = newBlocks cogroup repmatEntries where { block => block._1} isEqualTo { entry => entry
                  ._1} map {
                  (blocks, entries) =>
                    if(!blocks.hasNext){
                      throw new IllegalArgumentError("LoadMatrix coGroup phase must have at least one block")
                    }

                    val partition = blocks.next()._2

                    if (blocks.hasNext) {
                      throw new IllegalArgumentError("LoadMatrix coGroup phase must have at most one block")
                    }

                    SubmatrixBoolean(partition, (entries map { case (id, row, col, value) => (row, col, value)}).toSeq)
                }
                result.setName("Repmat: Repeated matrix")
                result

            }
            )
        }

      case l: linspace =>
        handle[linspace, (Scalar[Double], Scalar[Double], Scalar[Double])](
        l,
        { l => (evaluate[Scalar[Double]](l.start), evaluate[Scalar[Double]](l.end),
          evaluate[Scalar[Double]](l.numPoints))},
        { case (_,(start, end, numPoints)) =>
          val startEnd = start cross end map { (start, end) => (start, end)}
          startEnd.setName("Linspace: Start end pair")

          val blocks = numPoints flatMap { num =>
            val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, 1, num.toInt)
            for(partition <- partitionPlan.iterator) yield partition
          }
          blocks.setName("Linspace: New blocks")

          val result = blocks cross startEnd map { case (block, (start, end)) =>
            val spacing = (end-start)/(block.numTotalColumns-1)

            val entries = for(counter <- 0 until block.numColumns) yield {
              (0, counter + block.columnOffset, spacing * (counter + block.columnOffset)+start)
            }
            Submatrix(block, entries)
          }
          result.setName("Linspace: Linear spaced matrix")
          result

        }
        )

      case m: minWithIndex =>
        handle[minWithIndex, (Matrix, Scalar[Double])](
          m,
          {m => (evaluate[Matrix](m.matrix), evaluate[Scalar[Double]](m.dimension))},
          {
            case (_, (matrix, dimension)) =>
              val totalSizeDimension = matrix cross dimension map { (matrix, dim) =>
                if(dim == 1){
                  (1, matrix.totalColumns, dim)
                }else if(dim == 2){
                  (matrix.totalRows, 1, dim)
                }else{
                  throw new StratosphereExecutionError("minWithIndex does not support the dimension " + dim)
                }
              } reduceAll{ entries =>
                if(entries.hasNext)
                  entries.next()
                else{
                  throw new StratosphereExecutionError("minWithIndex result matrix has to have size distinct from (0," +
                    "0)")
                }
              }
              totalSizeDimension.setName("MinWithIndex: Total size with dimension")


              val minPerBlock = matrix cross dimension flatMap { (matrix, dim) =>
                if(dim == 1){
                  val minPerColumn = for(column <- 0 until matrix.cols) yield{
                    val (minRow, minValue) = matrix(::, column).iterator.minBy{ case (row, value) => value }
                    (column + matrix.columnOffset, minRow, minValue)
                  }
                  minPerColumn.toIterator
                }else if(dim == 2){
                  val minPerRow = for(row <- 0 until matrix.rows) yield {
                    val ((_,minCol), minValue) = matrix(row, ::).iterator.minBy { case (col, value) => value }
                    (row + matrix.rowOffset, minCol, minValue)
                  }
                  minPerRow.toIterator
                }else{
                  throw new StratosphereExecutionError("minWithIndex does not support the dimension "+ dim)
                }
              }
              minPerBlock.setName("MinWithIndex: Min per block")

              val minIndexValue = (minPerBlock groupBy { entry => entry._1}).combinableReduceGroup{ entries =>
                entries minBy ( x => x._3)
              }
              minIndexValue.setName("MinWithIndex: argmin and min value")

              val newBlocks = totalSizeDimension flatMap { case (rows, cols, _) =>
                val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, rows, cols)
                for(partition <- partitionPlan.toIterator) yield {
                  (partition.id, partition)
                }
              }
              newBlocks.setName("MinWithIndex: New blocks")
              val partitionedMinIndexValue = minIndexValue cross totalSizeDimension map { (mIdxValue, sizeDimension) =>
                val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, sizeDimension._1,
                  sizeDimension._2)

                if(sizeDimension._3 == 1){
                  val partitionId = partitionPlan.partitionId(0, mIdxValue._1)
                  (partitionId, 0, mIdxValue._1, mIdxValue._2, mIdxValue._3)
                }else if(sizeDimension._3 == 2){
                  val partitionId = partitionPlan.partitionId(mIdxValue._1, 0)
                  (partitionId, mIdxValue._1, 0, mIdxValue._2, mIdxValue._3)
                }else{
                  throw new StratosphereExecutionError("minWithIndex does not support the dimension "+ sizeDimension
                    ._3)
                }
              }
              partitionedMinIndexValue.setName("MinWithIndex: Partitioned argmin and min value")

              val minValues = newBlocks cogroup partitionedMinIndexValue where ( x => x._1) isEqualTo( x => x._1) map {
                (blocks, entries) =>
                  val minValues = entries map {
                    case (_, row, col, _, value) =>
                      (row,col,value)
                  }

                  if(!blocks.hasNext){
                    throw new IllegalArgumentError("MinWithIndex coGroup phase must have at least one block")
                  }

                  val partition = blocks.next()._2

                  if (blocks.hasNext) {
                    throw new IllegalArgumentError("MinWithIndex coGroup phase must have at most one block")
                  }

                  val minValuesSeq = minValues.toList
                  CellEntry(0,ValueWrapper(Submatrix(partition, minValuesSeq)))
              }
              minValues.setName("MinWithIndex: Min values cell entry")

              val minIndices = newBlocks cogroup partitionedMinIndexValue where ( x => x._1) isEqualTo( x => x._1) map
              { (blocks, entries) =>
                  val minIndices = entries map { case (_, row, col, minIndex, _) => (row,col,(minIndex+1).toDouble)}

                  if(!blocks.hasNext){
                    throw new IllegalArgumentError("MinWithIndex coGroup phase must have at least one block")
                  }

                  val partition = blocks.next()._2

                  if (blocks.hasNext) {
                    throw new IllegalArgumentError("MinWithIndex coGroup phase must have at most one block")
                  }

                  CellEntry(1,ValueWrapper(Submatrix(partition, minIndices.toSeq)))
              }
              minIndices.setName("MinWithIndex: Min indices cell entry")

              val result = minValues union minIndices
              result.setName("MinWithIndex: Cell array")
              result

          }
        )

      case p: pdist2 =>
        handle[pdist2, (Matrix, Matrix)](
          p,
          {p => (evaluate[Matrix](p.matrixA), evaluate[Matrix](p.matrixB)) },
          {
            case (_,(matrixA, matrixB)) =>
              val partialSqDiffs = matrixA join matrixB where { a => a.columnIndex } isEqualTo
                { b => b.columnIndex } map {
                (a,b) =>
                  val partitionPlan = new SquareBlockPartitionPlan(Configuration.BLOCKSIZE, a.totalRows, b.totalRows)
                  val newEntries = for(rowA <- 0 until a.rows; rowB <- 0 until b.rows) yield {
                    val diff = a(rowA,::) - b(rowB,::)
                    val diffsq = diff :^ 2.0
                    val summedDiff = breeze.linalg.sum(diffsq)
                    (rowA + a.rowOffset, rowB + b.rowOffset, summedDiff)
                  }

                  val partition = partitionPlan.getPartition(a.rowIndex, b.rowIndex)
                  Submatrix(partition, newEntries.toSeq)
              }
              partialSqDiffs.setName("Pdist2: Partial squared diffs")

              val pdist2 = partialSqDiffs groupBy( x => (x.rowIndex, x.columnIndex)) combinableReduceGroup {
                diffs =>
                  if(!diffs.hasNext){
                    throw new StratosphereExecutionError("Diffs is empty")
                  }

                  val first = diffs.next()

                  val summedDiffs = diffs.foldLeft(first)(_ + _)
                  summedDiffs :^ (0.5)
              }
              pdist2.setName("Pdist2: pair wise distance matrix")
              pdist2
          }
        )
    }
  }
}