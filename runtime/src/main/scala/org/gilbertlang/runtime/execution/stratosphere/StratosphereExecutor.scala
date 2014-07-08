package org.gilbertlang.runtime.execution.stratosphere

import _root_.breeze.linalg.{*, norm, min, max}
import breeze.stats.distributions.Gaussian
import org.apache.commons.logging.{LogFactory, Log}
import org.gilbertlang.runtime.Executor
import eu.stratosphere.api.scala.operators.{CsvInputFormat, CsvOutputFormat, DelimitedOutputFormat}
import eu.stratosphere.api.scala._
import org.gilbertlang.runtimeMacros.linalg.operators.{SubvectorImplicits, SubmatrixImplicits}
import scala.collection.convert.WrapAsScala
import org.gilbertlang.runtime.Operations._
import org.gilbertlang.runtime.Executables._
import org.gilbertlang.runtimeMacros.linalg._
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
import org.gilbertlang.runtime.Executables.FixpointIterationMatrix
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
import org.gilbertlang.runtime.execution.UtilityFunctions.binarize

@SerialVersionUID(1)
class StratosphereExecutor extends Executor with WrapAsScala with
SubmatrixImplicits with SubvectorImplicits  {
  import ImplicitConversions._

  type Matrix = DataSet[Submatrix]
  type BooleanMatrix = DataSet[BooleanSubmatrix]
  type Scalar[T] = DataSet[T]
  type CellArray = DataSet[CellEntry]
  private var tempFileCounter = 0
  private var iterationStatePlaceholderValue: Option[Matrix] = None
  private var iterationStatePlaceholderValueCellArray: Option[CellArray] = None
  private var convergencePreviousStateValue: Option[Matrix] = None
  private var convergenceCurrentStateValue: Option[Matrix] = None
  private var convergenceCurrentStateCellArrayValue: Option[CellArray] = None
  private var convergencePreviousStateCellArrayValue: Option[CellArray] = None

  implicit val doubleMatrixFactory = MatrixFactory.getDouble
  implicit val booleanMatrixFactory = MatrixFactory.getBoolean

  def getCWD: String = System.getProperty("user.dir")

  def newTempFileName(): String = {
    tempFileCounter += 1
    val separator = if(configuration.outputPath.getOrElse("").endsWith("/")) "" else "/"
    configuration.outputPath.getOrElse("") + separator + "gilbert" + tempFileCounter + ".output"
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
              List(matrix.write(completePathWithFilename, DelimitedOutputFormat(Submatrix.outputFormatter("\n", " ",
                configuration.verboseWrite),
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
              List(matrix.write(completePathWithFilename, DelimitedOutputFormat(BooleanSubmatrix.outputFormatter
                ("\n", " ", configuration.verboseWrite),
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
                mappedCell.setName("WriteCellArray: Unwrapped scalarRef Matrix(Double)")
                mappedCell.write(completePathWithFilename, DelimitedOutputFormat(Submatrix.outputFormatter("\n", " ", configuration.verboseWrite),
                  ""),
                  s"WriteCellArray(Matrix[Double], $completePathWithFilename)")
              case StringType =>
                val mappedCell =filtered map( x => x.wrappedValue[String])
                mappedCell.setName("WriteCellArray: Unwrapped scalarRef String")
                mappedCell.write(completePathWithFilename, CsvOutputFormat(),
                  s"WriteCellArray(String, $completePathWithFilename)")
              case DoubleType =>
                val mappedCell = filtered map(x => x.wrappedValue[Double])
                mappedCell.setName("WriteCellArray: Unwrapped scalarRef Double")
                mappedCell.write(completePathWithFilename, CsvOutputFormat(), s"WriteCellArray(Double," +
                  s"$completePathWithFilename)")
              case BooleanType =>
                val mappedCell = filtered map(x => x.wrappedValue[Boolean])
                mappedCell.setName("WriteCellArray: Unwrapped scalarRef Boolean")
                mappedCell.write(completePathWithFilename, CsvOutputFormat(), s"WriteCellArray(Boolean," +
                  s"$completePathWithFilename)")
              case tpe =>
                throw new StratosphereExecutionError(s"Cannot write cell entry of type $tpe.")
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
              List(scalar.write(completePathWithFilename, CsvOutputFormat[Double](),
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
          case tpe =>
            throw new StratosphereExecutionError("Cannot write scalar of type $tpe.")

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
                  val result = logicOperation match {
                    case And | SCAnd =>
                      val result = scalar cross matrix map { (scalar, submatrix) => submatrix :& scalar }
                      result.setName("SM: Logical And")
                      result
                    case Or | SCOr =>
                      val result = scalar cross matrix map { (scalar, submatrix) => submatrix :| scalar }
                      result.setName("SM: Logical Or")
                      result
                  }
                  if(configuration.compilerHints) {
                    if(configuration.preserveHint){
                      result.right.preserves(s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows,
                        s.totalColumns), s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows,
                        s.totalColumns))
                    }
                    result.uniqueKey(s => (s.rowIndex, s.columnIndex))
                  }
                  result
                })
          case operation: ArithmeticOperation =>
            handle[ScalarMatrixTransformation, (Scalar[Double], Matrix)](
          executable,
          { exec => (evaluate[Scalar[Double]](exec.scalar), evaluate[Matrix](exec.matrix)) },
          {
            case (_, (scalarDS, matrixDS)) =>
              val result = operation match {
                case Addition =>
                  val result = scalarDS cross matrixDS map { (scalar, submatrix) => submatrix + scalar }
                  result.setName("SM: Addition")
                  result
                case Subtraction =>
                  val result = scalarDS cross matrixDS map { (scalar, submatrix) => submatrix + -scalar }
                  result.setName("SM: Subtraction")
                  result
                case Multiplication =>
                  val result = scalarDS cross matrixDS map { (scalar, submatrix) => submatrix * scalar }
                  result.setName("SM: Multiplication")
                  result
                case Division =>
                  val result = scalarDS cross matrixDS map { (scalar, submatrix) =>
                    {
                      val partition = submatrix.getPartition
                      val result = Submatrix.init(partition, scalar)
                      result / submatrix
                    }
                  }
                  result.setName("SM: Division")
                  result
                case Exponentiation =>
                  val result = scalarDS cross matrixDS map { (scalar, submatrix) =>
                    val partition = submatrix.getPartition
                    val scalarMatrix = Submatrix.init(partition, scalar)
                    scalarMatrix :^ submatrix
                  }
                  result.setName("SM: CellwiseExponentiation")
                  result
              }
              if(configuration.compilerHints) {
                if(configuration.preserveHint){
                  result.right.preserves(s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows,
                    s.totalColumns), s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows, s.totalColumns))
                }
                result.uniqueKey(s => (s.rowIndex, s.columnIndex))
              }
              result
          })
          case operation: ComparisonOperation =>
            handle[ScalarMatrixTransformation, (Scalar[Double], Matrix)](
            executable,
            { exec => (evaluate[Scalar[Double]](exec.scalar), evaluate[Matrix](exec.matrix)) },
            {
              case (_, (scalar, matrix)) =>
                val result = operation match {
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
                if(configuration.compilerHints) {
                  if(configuration.preserveHint){
                    result.right.preserves(s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows,
                      s.totalColumns), s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows,
                      s.totalColumns))
                  }
                  result.uniqueKey(s => (s.rowIndex, s.columnIndex))
                }
                result
            })

        }

      case executable: MatrixScalarTransformation =>
        executable.operation match {
          case logicOperation: LogicOperation =>
            handle[MatrixScalarTransformation, (BooleanMatrix, Scalar[Boolean])](
                executable,
                {exec => (evaluate[BooleanMatrix](exec.matrix), evaluate[Scalar[Boolean]](exec.scalar))},
                { case (_, (matrix, scalar)) =>
                  val result = logicOperation match {
                    case And | SCAnd =>
                      val result = matrix cross scalar map { (submatrix, scalar) => submatrix :& scalar }
                      result.setName("MS: Logical And")
                      result
                    case Or | SCOr =>
                      val result = matrix cross scalar map { (submatrix, scalar) => submatrix :| scalar }
                      result.setName("MS: Logical Or")
                      result
                  }
                  if(configuration.compilerHints) {
                    if(configuration.preserveHint){
                      result.left.preserves(s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows,
                        s.totalColumns), s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows,
                        s.totalColumns))
                    }
                    result.uniqueKey(s => (s.rowIndex, s.columnIndex))
                  }
                  result
                })
          case operation : ArithmeticOperation =>
            handle[MatrixScalarTransformation, (Matrix, Scalar[Double])](
          executable,
          { exec => (evaluate[Matrix](exec.matrix), evaluate[Scalar[Double]](exec.scalar)) },
          {
            case (_, (matrixDS, scalarDS)) =>
              val result = operation match {
                case Addition =>
                  val result = matrixDS cross scalarDS map { (submatrix, scalar) => submatrix + scalar }
                  result.setName("MS: Addition")
                  result
                case Subtraction =>
                  val result = matrixDS cross scalarDS map { (submatrix, scalar) => submatrix - scalar }
                  result.setName("MS: Subtraction")
                  result
                case Multiplication =>
                  val result = matrixDS cross scalarDS map { (submatrix, scalar) => submatrix * scalar }
                  result.setName("MS: Multiplication")
                  result
                case Division =>
                  val result = matrixDS cross scalarDS map { (submatrix, scalar) => submatrix / scalar }
                  result.setName("MS: Division")
                  result
                case Exponentiation =>
                  val result = matrixDS cross scalarDS map { (submatrix, scalar) => submatrix :^ scalar}
                  result.setName("MS: Exponentiation")
                  result
              }
              if(configuration.compilerHints) {
                if(configuration.preserveHint){
                  result.left.preserves(s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows,
                    s.totalColumns), s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows,
                    s.totalColumns))
                }
                result.uniqueKey(s => (s.rowIndex, s.columnIndex))
              }
              result
          })
          case operation : ComparisonOperation =>
            handle[MatrixScalarTransformation, (Matrix, Scalar[Double])](
            executable,
            { exec => (evaluate[Matrix](exec.matrix), evaluate[Scalar[Double]](exec.scalar)) },
            {
              case (_, (matrix, scalar)) =>
                val result = operation match {
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
                if(configuration.compilerHints){
                  if(configuration.preserveHint){
                    result.left.preserves(s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows,
                      s.totalColumns), s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows,
                      s.totalColumns))
                  }
                  result.uniqueKey(s => (s.rowIndex, s.columnIndex))
                }
                result
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
                  case And  =>
                    val result = left cross right map { (left, right) => left & right }
                    result.setName("SS: Logical And")
                    result
                  case Or =>
                    val result = left cross right map { (left, right) => left | right }
                    result.setName("SS: Logical Or")
                    result
                  case SCAnd  =>
                    val result = left cross right map { (left, right) => left && right }
                    result.setName("SS: Logical And")
                    result
                  case SCOr =>
                    val result = left cross right map { (left, right) => left || right }
                    result.setName("SS: Logical Or")
                    result
                }
              }
            )
          case operation: ArithmeticOperation =>
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
                    case Exponentiation =>
                      val result = left cross right map { (left, right) => math.pow(left,right)}
                      result.setName("SS: Exponentiation")
                      result
                  }
              })
          case operation: MinMax =>
            handle[ScalarScalarTransformation, (Scalar[Double], Scalar[Double])](
            executable,
            { exec => (evaluate[Scalar[Double]](exec.left), evaluate[Scalar[Double]](exec.right)) },
            {
              case (_, (left, right)) =>
                operation match {
                  case Maximum =>
                    val result = left union right combinableReduceAll { elements => elements.max }
                    result.setName("SS: Maximum")
                    result
                  case Minimum =>
                    val result = left union right combinableReduceAll { elements => elements.min }
                    result.setName("SS: Minimum")
                    result
                }
            })
          case operation: ComparisonOperation =>
            handle[ScalarScalarTransformation, (Scalar[Double], Scalar[Double])](
            executable,
            { exec => (evaluate[Scalar[Double]](exec.left), evaluate[Scalar[Double]](exec.right)) },
            {
              case (_, (left, right)) =>
                operation match {
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
                  matrix map { x => max(x) } combinableReduceAll
                    { elements => elements.max }
                case Minimum =>
                  matrix map { x => min(x) } combinableReduceAll
                    { elements => elements.min }
                case Norm2 =>
                  matrix map { x => _root_.breeze.linalg.sum(x:*x) } combinableReduceAll
                    { x => x.fold(0.0)(_ + _) } map
                    { x => math.sqrt(x) }
                case SumAll =>
                  val blockwiseSum = matrix map { x => _root_.breeze.linalg.sum(x) }
                  blockwiseSum.setName("Aggregate Matrix: Blockwise sum.")
                  val result = blockwiseSum combinableReduceAll( sums => sums.foldLeft(0.0)(_ + _))
                  result.setName("Aggregate Matrix: Sum all")
                  result
              }
            }
          })

      case executable: UnaryScalarTransformation =>
        handle[UnaryScalarTransformation, Scalar[Double]](
          executable,
          { exec => evaluate[Scalar[Double]](exec.scalar) },
          { (exec, scalarDS) =>
            {
              exec.operation match {
                case Minus =>
                  scalarDS map { x => -x }
                case Binarize =>
                  scalarDS map { binarize }
                case Abs =>
                  scalarDS map { value => math.abs(value) }
              }
            }
          })

      case executable: scalar =>
        handle[scalar, Unit](
          executable,
          { _ => },
          { (exec, _) =>
            val result = CollectionDataSource(List(exec.value))

            if(configuration.compilerHints){
              result.outputCardinality = 1
            }

            result

          })

      case executable: boolean =>
        handle[boolean, Unit](
            executable,
            {_ => },
            { (exec, _) =>
              val result = CollectionDataSource(List(exec.value))

              if(configuration.compilerHints){
                result.outputCardinality = 1
              }

              result
            })

      case executable: string =>
        handle[string, Unit](
          executable,
          { _ => },
          { (exec, _) =>
            val result = CollectionDataSource(List(exec.value))
            if(configuration.compilerHints){
              result.outputCardinality = 1
            }

            result
          })

      case executable: CellwiseMatrixTransformation =>
        handle[CellwiseMatrixTransformation, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (exec, matrixDS) =>
            {
              val result = exec.operation match {
                case Minus =>
                  matrixDS map { submatrix => submatrix * -1.0}
                case Binarize =>
                  matrixDS map { submatrix =>
                    {
                      submatrix.mapActiveValues( binarize)
                    }
                  }
                case Abs =>
                  matrixDS map { submatrix => submatrix.mapActiveValues( value => math.abs(value))}
              }
              if(configuration.compilerHints){
                if(configuration.preserveHint){
                  result.preserves(s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows,
                    s.totalColumns), s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows,
                    s.totalColumns))
                }
                result.uniqueKey(s=> (s.rowIndex, s.columnIndex))
              }
              result
            }
          })

      case executable: CellwiseMatrixMatrixTransformation =>
        executable.operation match {
          case logicOperation: LogicOperation =>
            handle[CellwiseMatrixMatrixTransformation, (BooleanMatrix, BooleanMatrix)](
                executable,
                { exec => (evaluate[BooleanMatrix](exec.left), evaluate[BooleanMatrix](exec.right))},
                { case (_, (left, right)) =>
                  val result = logicOperation match {
                    case And | SCAnd =>
                      val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) => left :& right }
                      result.setName("MM: Logical And")
                      result
                    case Or | SCOr =>
                      val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                      { y => (y.rowIndex, y.columnIndex) } map
                      { (left, right) => left :| right }
                      result.setName("MM: Logical Or")
                      result
                  }
                  if(configuration.compilerHints){
                    result.uniqueKey(s=> (s.rowIndex, s.columnIndex))
                  }
                  result
                })
          case operation: ArithmeticOperation =>
            handle[CellwiseMatrixMatrixTransformation, (Matrix, Matrix)](
          executable,
          { exec => (evaluate[Matrix](exec.left), evaluate[Matrix](exec.right)) },
          {
            case (_ , (left, right)) =>
              val result = operation match {
                case Addition =>
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } isEqualTo
                    { y => (y.rowIndex, y.columnIndex) } map
                    { (left, right) =>
                      val result = left + right
                      result
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
                case Exponentiation =>
                  val result = left join right where {x => (x.rowIndex, x.columnIndex)} isEqualTo { y => (y.rowIndex,
                   y.columnIndex)} map { (left, right) => left :^ right }
                  result.setName("MM: Exponentiation")
                  result
              }
              if(configuration.compilerHints){
                result.uniqueKey(s=> (s.rowIndex, s.columnIndex))
              }
              result
          })
          case operation: MinMax =>
            handle[CellwiseMatrixMatrixTransformation, (Matrix, Matrix)](
            executable,
            { exec => (evaluate[Matrix](exec.left), evaluate[Matrix](exec.right)) },
            {
              case (_ , (left, right)) =>
                val result = operation match {
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
                }
                if(configuration.compilerHints){
                  result.uniqueKey(s=> (s.rowIndex, s.columnIndex))
                }
                result
            })
          case operation: ComparisonOperation =>
            handle[CellwiseMatrixMatrixTransformation, (Matrix, Matrix)](
            executable,
            { exec => (evaluate[Matrix](exec.left), evaluate[Matrix](exec.right)) },
            {
              case (_ , (left, right)) =>
                val result = operation match {
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
                if(configuration.compilerHints){
                  result.uniqueKey(s=> (s.rowIndex, s.columnIndex))
                }
                result
            })
        }

      case executable: MatrixMult =>
        handle[MatrixMult, (Matrix, Matrix)](
          executable,
          { exec => (evaluate[Matrix](exec.left), evaluate[Matrix](exec.right)) },
          {
            case (_, (left, right)) =>
              val joinedBlocks = left join right where { leftElement => leftElement.columnIndex } isEqualTo
                { rightElement => rightElement.rowIndex } map
                { (left, right) =>
                  StratosphereExecutor.log.info("Start matrix multiplication")
                  val result = left * right
                  StratosphereExecutor.log.info("End matrix multiplication")

                  result
                }

              val reduced = joinedBlocks groupBy
                { element => (element.rowIndex, element.columnIndex) } combinableReduceGroup
                { elements =>
                  {
                    val element = elements.next()
                    val copied= element.copy
                    val result = elements.foldLeft(copied)({ _ + _ })
                    result
                  }
                }
              if(configuration.compilerHints){
                if(configuration.preserveHint){
                  reduced.preserves(s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows,
                    s.totalColumns), s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows, s.totalColumns))
                }
                reduced.uniqueKey(s => (s.rowIndex, s.columnIndex))
              }
              reduced
          })

      case executable: Transpose =>
        handle[Transpose, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (_, matrixDS) =>
            val result = matrixDS map { matrix => matrix.t }
            if(configuration.compilerHints){
              result.uniqueKey(s => (s.rowIndex, s.columnIndex))
            }
            result
          })

      case executable: VectorwiseMatrixTransformation =>
        handle[VectorwiseMatrixTransformation, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (exec, matrix) =>
            {
              exec.operation match {
                case NormalizeL1 =>
                  val blockwiseNorm = matrix map {
                    submatrix =>
                      norm(submatrix( * , ::),1)
                  }
                  blockwiseNorm.setName("VM: Blockwise L1 norm")

                  val l1norm = blockwiseNorm groupBy (subvector => subvector.index) combinableReduceGroup {
                    subvectors =>
                      {
                        val firstElement = subvectors.next().copy
                        subvectors.foldLeft(firstElement)(_ + _)
                      }
                  }
                  l1norm.setName("VM: L1 norm")

                  val normedMatrix =  l1norm join
                    matrix where { l1norm => l1norm.index } isEqualTo { submatrix => submatrix.rowIndex } map
                    { (l1norm, submatrix) =>
                      val result = submatrix.copy
                      for(col <- submatrix.colRange)
                        result(::, col) :/= l1norm

                      result
                    }
                  normedMatrix.setName("VM: L1 normed matrix")

                  if(configuration.compilerHints){
                    if(configuration.preserveHint){
                      l1norm.preserves(v => (v.index, v.offset, v.totalEntries), v=> (v.index, v.offset, v.totalEntries))

                      normedMatrix.right.preserves(s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset,
                        s.totalRows, s.totalColumns), s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset,
                        s.totalRows, s.totalColumns))
                    }
                    l1norm.uniqueKey(v => v.index)
                    normedMatrix.uniqueKey(s => (s.rowIndex, s.columnIndex))
                  }

                  normedMatrix
                case Maximum =>
                  val blockwiseMax = matrix map { submatrix => max(submatrix(*, ::)) }
                  blockwiseMax.setName("VM: Blockwise maximum")

                  val maximum = blockwiseMax groupBy
                    { subvector => subvector.index } combinableReduceGroup { subvectors =>
                      val firstElement = subvectors.next().copy
                      subvectors.foldLeft(firstElement) { numerics.max(_, _) }
                    }
                  maximum.setName("VM: vectorwise maximum")

                  val matrixResult = maximum map { subvector => subvector.asMatrix }
                  matrixResult.setName("VM: vectorwise maximum matrix form")

                  if(configuration.compilerHints){
                    if(configuration.preserveHint){
                      maximum.preserves(v => (v.index, v.offset, v.totalEntries), v=> (v.index, v.offset, v.totalEntries))
                    }
                    maximum.uniqueKey(v => v.index)

                    matrixResult.uniqueKey(s => (s.rowIndex, s.columnIndex))
                  }

                  matrixResult
                case Minimum =>
                  val blockwiseMin = matrix map { submatrix => min(submatrix(*, ::)) }
                  blockwiseMin.setName("VM: Blockwise minimum")

                  val minimum = blockwiseMin groupBy
                    { subvector => subvector.index } combinableReduceGroup { subvectors =>
                      val firstElement = subvectors.next().copy
                      subvectors.foldLeft(firstElement) { numerics.min(_, _) }
                    }
                  minimum.setName("VM: Vectorwise minimum")

                  val matrixResult = minimum map { subvector => subvector.asMatrix }
                  matrixResult.setName("VM: Vectorwise minimum in matrix form")

                  if(configuration.compilerHints){
                    if(configuration.preserveHint){
                      minimum.preserves(v => (v.index, v.offset, v.totalEntries), v=> (v.index, v.offset, v.totalEntries))
                    }
                    minimum.uniqueKey(v => v.index)
                    matrixResult.uniqueKey(s => (s.rowIndex, s.columnIndex))
                  }

                  matrixResult
                case Norm2 =>
                  val squaredValues = matrix map { submatrix => submatrix :^ 2.0 }
                  squaredValues.setName("VWM: Norm2 squared values")

                  val blockwiseSum = squaredValues map { submatrix => _root_.breeze.linalg.sum(submatrix(*,
                    ::)) }
                  blockwiseSum.setName("VM: Norm2 blockwise sum")

                  val sumSquaredValues = blockwiseSum groupBy
                    { subvector => subvector.index } combinableReduceGroup
                    { subvectors =>
                      val firstSubvector = subvectors.next().copy
                      subvectors.foldLeft(firstSubvector)(_ + _)
                    }
                  sumSquaredValues.setName("VWM: Norm2 sum squared values")

                  val result = sumSquaredValues map {
                    sqv =>
                      sqv.asMatrix mapActiveValues { value => math.sqrt(value) }
                  }
                  result.setName("VWM: Norm2")

                  if(configuration.compilerHints){
                    if(configuration.preserveHint){
                      squaredValues.preserves(s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset,
                        s.totalRows, s.totalColumns), s=> (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset,
                        s.totalRows, s.totalColumns))

                      sumSquaredValues.preserves(v => (v.index, v.offset, v.totalEntries), v => (v.index, v.offset,
                        v.totalEntries))
                    }

                    squaredValues.uniqueKey(s => (s.rowIndex, s.columnIndex))


                    sumSquaredValues.uniqueKey(v => v.index)

                    result.uniqueKey(s => (s.rowIndex, s.columnIndex))
                  }

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
              val rowsColsPair = rowsCols map {
                (rows, cols) => (rows.toInt, cols.toInt)
              }
              rowsColsPair.setName("LoadMatrix: Rows and Cols pair")

              val blocks = rowsColsPair flatMap { case (numRows, numCols) =>
                val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, numRows, numCols)
                for (partition <- partitionPlan.iterator) yield {
                  (partition.id, partition)
                }
              }
              blocks.setName("LoadMatrix: Partition blocks")

              val partitionedData = rowsColsPair cross source map {
                case ((numRows, numCols), (row, column, value)) =>
                  val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, numRows, numCols)
                  (partitionPlan.partitionId(row-1, column-1), row-1, column-1, value)
              }
              partitionedData.setName("LoadMatrix: Partitioned data")

              val loadedMatrix = partitionedData cogroup blocks where { entry => entry._1 } isEqualTo { block => block
              ._1 } map {
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
              loadedMatrix.setName("LoadMatrix: Loaded matrix")

              if(configuration.compilerHints){
                rowsColsPair.outputCardinality = 1

                blocks.uniqueKey(p => p._1)

                loadedMatrix.uniqueKey(s => (s.rowIndex, s.columnIndex))
              }

              loadedMatrix
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
              val partitions = rows cross columns flatMap { (rows, columns) =>
                val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rows.toInt, columns.toInt)
                partitionPlan.iterator
              }
              partitions.setName("Ones: Partitions")

              //reduceGroup to distributed the partitions across the worker nodes
              val distributedPartitions = partitions.groupBy{p => p.id}.reduceGroup{ps => ps.next}
              distributedPartitions.setName("Ones: Distributed partitions")

              val result = distributedPartitions map { p =>
                Submatrix.init(p, 1.0)
              }

              result.setName("Ones")

              if(configuration.compilerHints){
                result.uniqueKey(s => (s.rowIndex, s.columnIndex))
              }

              result
          })

      case executable: zeros =>
        handle[zeros, (Scalar[Double], Scalar[Double])](
            executable,
            { exec => (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numCols))},
            { case (_, (rows, columns)) =>
              val partitions = rows cross columns flatMap { (rows, columns) =>
                val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rows.toInt, columns.toInt)

                partitionPlan.iterator
              }
              partitions.setName("Zeros: Partitions")

              //reduceGroup to distribute the partitions across the worker nodes
              val distributedPartitions = partitions.groupBy(p => p.id).reduceGroup(ps => ps.next)
              distributedPartitions.setName("Zeros: Distributed partitions")

              val result = distributedPartitions map { p => Submatrix(p)}

              result.setName(s"Zeros")

              if(configuration.compilerHints){
                distributedPartitions.uniqueKey(p => p.id)
                result.uniqueKey(s => (s.rowIndex, s.columnIndex))
              }

              result
            })

      case executable: eye =>
        handle[eye, (Scalar[Double], Scalar[Double])](
            executable,
            { exec => (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numCols))},
            { case (_, (rows, columns)) =>
              val partitions = rows cross columns flatMap { (rows, columns) =>
                val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rows.toInt, columns.toInt)
                partitionPlan.iterator
              }
              partitions.setName("Eye: Partitions")

              //reduceGroup to distribute partitions across worker nodes
              val distributedPartitions = partitions.groupBy{p => p.id}.reduceGroup{
                ps =>
                  val result = ps.next
                  val s = ps.size
                  result
              }
              distributedPartitions.setName("Eye: Distributed partitions")

              val result = distributedPartitions.map{
                p =>
                  Submatrix.eye(p)
              }

              result.setName("Eye")

              if(configuration.compilerHints){
                distributedPartitions.uniqueKey(p => p.id)
                result.uniqueKey(s => (s.rowIndex, s.columnIndex))
              }

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
            case (_, (rowsDS, colsDS, meanDS, stdDS)) =>
              val partitions = rowsDS cross colsDS flatMap {
                (rows, cols) =>
                  val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rows.toInt,
                    cols.toInt)
                  partitionPlan.iterator
              }
              partitions.setName("Randn: Partitions")

              //reduceGroup to distribute the partitions across the worker nodes
              val distributedPartitions = partitions.groupBy{p => p.id}.reduceGroup{ ps => ps.next}
              distributedPartitions.setName("Randn: Distributed partitions")

              val meanStd = meanDS cross stdDS map { (mean, std) => (mean, std) }
              meanStd.setName("Randn: mean and std pair")

              val randomBlocks = distributedPartitions cross meanStd map {
                case (partition, (mean, std)) =>
                  Submatrix.rand(partition, Gaussian(mean, std))

              }

              randomBlocks.setName("Randn: Random submatrices")

              if(configuration.compilerHints){
                distributedPartitions.uniqueKey(p => p.id)
                meanStd.outputCardinality = 1

                randomBlocks.uniqueKey(s => (s.rowIndex, s.columnIndex))
              }

              randomBlocks
          })

      case executable: sprand =>
        handle[sprand, (Scalar[Double], Scalar[Double], Scalar[Double], Scalar[Double], Scalar[Double])](
        executable,
        { exec =>
          (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numCols),
            evaluate[Scalar[Double]](exec.mean), evaluate[Scalar[Double]](exec.std),
            evaluate[Scalar[Double]](exec.level))
        },
        {
          case (_, (rowsDS, colsDS, meanDS, stdDS, levelDS)) =>
            val partitions = rowsDS cross colsDS flatMap { (rows, cols) =>
              val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rows.toInt, cols.toInt)

              partitionPlan.iterator
            }
            partitions.setName("Sprand: Random partitions")

            //reduceGroup to distribute the partitions across the worker nodes
            val distributedPartitions = partitions.groupBy(p => p.id).reduceGroup(ps => ps.next)
            distributedPartitions.setName("Sprand: Disitributed partitions")

            val meanStd = meanDS cross stdDS map { case (mean, std) => (mean, std)}
            meanStd.setName("Sprand: mean and std combined")

            val meanStdLevel = meanStd cross stdDS map { case ((mean, std), level) => (mean, std, level)}
            meanStdLevel.setName("Sprand: Mean, std and level combined")

            val randomSparseBlocks = distributedPartitions cross meanStdLevel map {
              case (partition, (mean, std, level)) =>
                val rand = Gaussian(mean, std)
                Submatrix.sprand(partition, rand, level)
            }

            randomSparseBlocks.setName("Sprand: Sparse random blocks")

            if(configuration.compilerHints){
              meanStd.outputCardinality = 1
              meanStdLevel.outputCardinality = 1

              distributedPartitions.uniqueKey(p => p.id)

              randomSparseBlocks.uniqueKey(s => (s.rowIndex, s.columnIndex))
            }

            randomSparseBlocks
        }
        )

      case executable: adaptiveRand =>
        handle[adaptiveRand, (Scalar[Double], Scalar[Double], Scalar[Double], Scalar[Double], Scalar[Double])](
        executable,
        { input =>
          (evaluate[Scalar[Double]](input.numRows), evaluate[Scalar[Double]](input.numColumns),
            evaluate[Scalar[Double]](input.mean), evaluate[Scalar[Double]](input.std),
            evaluate[Scalar[Double]](input.level))
        },
        {
          case (_, (rowsDS, colsDS, meanDS, stdDS, levelDS)) =>
            val partitions = rowsDS cross colsDS flatMap { (rows, cols) =>
              val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rows.toInt, cols.toInt)
              partitionPlan.iterator
            }
            partitions.setName("AdaptiveRand: Partitions")

            //reduceGroup to distribute the partitions explicitely across the worker nodes
            val distributedPartitions = partitions.groupBy(p => p.id).reduceGroup(ps => ps.next)
            distributedPartitions.setName("AdaptiveRand: Distributed partitions")

            val meanStd = meanDS cross stdDS map {(mean, std) => (mean, std)}
            meanStd.setName("AdaptiveRand: Mean and std pair")

            val meanStdLevel = meanStd cross levelDS map { case ((mean, std), level) => (mean, std, level)}
            meanStdLevel.setName("AdaptiveRand: Mean, std and level triple")

            val result = distributedPartitions cross meanStdLevel map {
              case (partition, (mean, std, level)) =>
                val random = Gaussian(mean, std)
                Submatrix.adaptiveRand(partition, random, level, configuration.densityThreshold)
            }

            if(configuration.compilerHints){
              distributedPartitions.uniqueKey(p => p.id)
              meanStd.outputCardinality = 1
              meanStdLevel.outputCardinality = 1
              result.uniqueKey(s => (s.rowIndex, s.columnIndex))
            }

            result
        }
        )

      case executable: spones =>
        handle[spones, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (_, matrix) =>
            {
              val result = matrix map { submatrix =>
                submatrix.mapActiveValues(binarize)
              }

              result.setName("Spones")

              if(configuration.compilerHints){
                result.uniqueKey(s => (s.rowIndex, s.columnIndex))
              }

              result
            }
          })

      case executable: sumRow =>
        handle[sumRow, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (_, matrix) =>
            {
              val blockwiseSum = matrix map { submatrix => _root_.breeze.linalg.sum(submatrix(*, ::)) }
              blockwiseSum.setName("SumRow: Blockwise rowwise sum")

              val rowwiseSum = blockwiseSum groupBy
                { subvector => subvector.index } combinableReduceGroup
                { subvectors =>
                  val firstSubvector = subvectors.next().copy
                  subvectors.foldLeft(firstSubvector)(_ + _)
                }
              rowwiseSum.setName("SumRow: Row-wise sum")

              val matrixResult = rowwiseSum map { subvector => subvector.asMatrix }
              matrixResult.setName("SumRow: Row-wise sum in matrix form")

              if(configuration.compilerHints){

                rowwiseSum.uniqueKey(v => v.index)
                rowwiseSum.preserves(v => (v.index, v.offset, v.totalEntries), v => (v.index, v.offset, v.totalEntries))

                matrixResult.uniqueKey(s => (s.rowIndex, s.columnIndex))
              }

              matrixResult
            }
          })

      case executable: sumCol =>
        handle[sumCol, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (_, matrix) =>
            {
              val blockwiseSum = matrix map { submatrix => _root_.breeze.linalg.sum(submatrix(::, *)) }
              blockwiseSum.setName("SumCol: Blockwise column sum")

              val colwiseSum = blockwiseSum groupBy
                { submatrix => submatrix.columnIndex } combinableReduceGroup
                { subvectors =>
                  val firstSubvector = subvectors.next().copy
                  subvectors.foldLeft(firstSubvector)(_ + _)
                }
              colwiseSum.setName("SumCol: Column sum")

              if(configuration.compilerHints){
                if(configuration.preserveHint){
                  blockwiseSum.preserves(s => (s.columnIndex, s.columnOffset, s.totalColumns), s=> (s.columnIndex,
                    s.columnOffset, s.totalColumns))

                  colwiseSum.preserves(s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows,
                    s.totalColumns), s => (s.rowIndex, s.columnIndex, s.rowOffset, s.columnOffset, s.totalRows,
                    s.totalColumns))
                }

                colwiseSum.uniqueKey(s => (s.rowIndex, s.columnIndex))
              }

              colwiseSum
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
                  entries.setName("Diag: Row vector input")

                  val result = entries cross matrix map {
                    case ((rowIndex, numRows, rowOffset), submatrix) =>
                      val partition = Partition(-1, rowIndex, submatrix.columnIndex, numRows, submatrix.cols,
                        rowOffset, submatrix.columnOffset, submatrix.totalColumns, submatrix.totalColumns)

                      if (submatrix.columnIndex == rowIndex) {
                        val result = Submatrix(partition, submatrix.cols)

                        for (index <- submatrix.colRange) {
                          result.update(index, index, submatrix(0, index))
                        }

                        result
                      }else{
                        Submatrix(partition)
                      }
                  }
                  result.setName("Diag: Result diagonal matrix of a row vector")

                  if(configuration.compilerHints){
                    result.uniqueKey(s => (s.rowIndex, s.columnIndex))
                  }

                  result
                case (_, Some(1)) =>
                  val entries = matrix map {
                    submatrix =>
                      (submatrix.rowIndex, submatrix.rows, submatrix.rowOffset)
                  }
                  entries.setName("Diag: Column vector input")

                  val result = entries cross matrix map {
                    case ((columnIndex, numColumns, columnOffset), submatrix) =>
                      val partition = Partition(-1, submatrix.rowIndex, columnIndex, submatrix.rows, numColumns,
                        submatrix.rowOffset, columnOffset, submatrix.totalRows, submatrix.totalRows)



                      if (submatrix.rowIndex == columnIndex) {
                        val result = Submatrix(partition, submatrix.rows)

                        for (index <- submatrix.rowRange) {
                          result.update(index, index, submatrix(index, 0))
                        }

                        result
                      }else{
                        Submatrix(partition)
                      }
                  }
                  result.setName("Diag: Result diagonal matrix of a column vector")

                  if(configuration.compilerHints){
                    result.uniqueKey(s => (s.rowIndex, s.columnIndex))
                  }
                  result
                case _ =>
                  val partialDiagResults = matrix map { submatrix =>
                    val partition = Partition(-1, submatrix.rowIndex, 0, submatrix.rows, 1, submatrix.rowOffset, 0,
                      submatrix.totalRows, 1)

                    val result = Submatrix(partition, submatrix.rows)

                    Submatrix.containsDiagonal(partition) match {
                      case Some(startIndex) =>
                        for(index <- startIndex until math.min(submatrix.rowOffset+submatrix.rows,
                          submatrix.columnOffset + submatrix.cols)){
                          result.update(index,0,submatrix(index,index))
                        }
                      case None => ;
                    }

                    result
                  }
                  partialDiagResults.setName("Diag: Extract diagonal")

                  val result = partialDiagResults groupBy { partialResult => partialResult.rowIndex } combinableReduceGroup {
                    results =>
                      val result = results.next().copy
                      results.foldLeft(result)(_ + _)
                  }
                  result.setName("Diag: ")

                  if(configuration.compilerHints){
                    result.uniqueKey(s => (s.rowIndex, s.columnIndex))
                  }

                  result
              }
            }
          })

      case executable: FixpointIterationMatrix =>
        handle[FixpointIterationMatrix, (Matrix, Scalar[Double])](
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

            if(exec.convergencePlan != null){
              val terminationFunction = (prev: Matrix, cur: Matrix) => {
                val oldPreviousState = convergencePreviousStateValue
                val oldCurrentState = convergenceCurrentStateValue
                convergencePreviousStateValue = Some(prev)
                convergenceCurrentStateValue = Some(cur)
                val result = evaluate[Scalar[Boolean]](exec.convergencePlan)

                convergencePreviousStateValue = oldPreviousState
                convergenceCurrentStateValue = oldCurrentState
                result filter { b => !b}
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
          case None => throw new StratosphereExecutionError("The iteration state placeholder scalarRef was not set yet.")
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
            result map { x => x }
          }

          var iteration: CellArray = null

          if(exec.convergencePlan != null){
            val terminationFunction = (prev: CellArray, cur: CellArray) => {
              val oldPreviousState = convergencePreviousStateCellArrayValue
              val oldCurrentState = convergenceCurrentStateCellArrayValue
              convergencePreviousStateCellArrayValue = Some(prev)
              convergenceCurrentStateCellArrayValue = Some(cur)

              val result = evaluate[Scalar[Boolean]](exec.convergencePlan)

              convergencePreviousStateCellArrayValue = oldPreviousState
              convergenceCurrentStateCellArrayValue = oldCurrentState

              result filter { b => !b }
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
          case None => throw new StratosphereExecutionError("The iteration state placeholder scalarRef was not set yet.")
        }

      case executable: sum =>
        handle[sum, (Matrix, Scalar[Double])](
          executable,
          { exec => (evaluate[Matrix](exec.matrix), evaluate[Scalar[Double]](exec.dimension)) },
          {
            case (_, (matrix, scalar)) =>
              val partialSum = scalar cross matrix map { (scalar, submatrix) =>
                if (scalar == 1) {
                  (submatrix.columnIndex, _root_.breeze.linalg.sum(submatrix(::, *)))
                } else {
                  (submatrix.rowIndex, _root_.breeze.linalg.sum(submatrix(*, ::)).asMatrix)
                }
              }
              partialSum.setName("Sum: Partial sum")

              val pairIDSubmatrix = partialSum groupBy { case (group, submatrix) => group } combinableReduceGroup {
                submatrices =>
                  val firstSubmatrix = submatrices.next()
                  (firstSubmatrix._1, submatrices.foldLeft(firstSubmatrix._2.copy)(_ + _._2))
              }
              pairIDSubmatrix.setName("Sum: (ID, sum)")

              val result = pairIDSubmatrix map {
                case (_, submatrix) =>
                  submatrix
              }
              result.setName("Sum: Final result")

              if(configuration.compilerHints){
                if(configuration.preserveHint){
                  pairIDSubmatrix.preserves(p => p._1, r => r._1)
                }
                pairIDSubmatrix.uniqueKey(p => p._1)

                result.neglects(p => p._1)
                result.uniqueKey(s => (s.rowIndex, s.columnIndex))
              }
              result
          })

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
                val result = exec.elements(index).getType match {
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
                result.setName(s"CellEntry($index)")
                result
            }

            val firstEntry = cellArrayEntries.head
            val result = cellArrayEntries.tail.foldLeft(firstEntry)(_ union _)
            result.setName("CellArray")

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
            case Unknown =>
              throw new StratosphereExecutionError("Cannot reference scalar of type Unknown.")
            case Void =>
              throw new StratosphereExecutionError("Cannot reference scalar of type Void.")
            case ScalarType =>
              throw new StratosphereExecutionError("Cannot reference scalar of plain type ScalarType.")
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
              filtered map { x => x.wrappedValue[BooleanSubmatrix]}
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

      case parameter: CellArrayParameter =>
        throw new StratosphereExecutionError("Parameter found. Cannot execute parameters.")

      case ConvergencePreviousStatePlaceholder =>
        convergencePreviousStateValue match {
          case Some(matrix) => matrix
          case None => throw new StratosphereExecutionError("Convergence previous state scalarRef has not been set.")
        }

      case ConvergenceCurrentStatePlaceholder =>
        convergenceCurrentStateValue match {
          case Some(matrix) => matrix
          case None => throw new StratosphereExecutionError("Convergence current state scalarRef has not been set.")
        }

      case placeholder: ConvergenceCurrentStateCellArrayPlaceholder =>
        convergenceCurrentStateCellArrayValue match {
          case Some(cellArray) => cellArray
          case None => throw new StratosphereExecutionError("Convergence current state cell array scalarRef has not been " +
            "set.")
        }

      case placeholder: ConvergencePreviousStateCellArrayPlaceholder =>
        convergencePreviousStateCellArrayValue match {
          case Some(cellArray) => cellArray
          case None => throw new StratosphereExecutionError("Convergence previous state cell array scalarRef has not been" +
            " set.")
        }

      case typeConversion: TypeConversionScalar =>
        (typeConversion.sourceType, typeConversion.targetType) match {
          case (BooleanType, DoubleType) =>
            handle[TypeConversionScalar, Scalar[Boolean]](
            typeConversion,
            { input => evaluate[Scalar[Boolean]](input.scalar)},
            { (_, scalar) =>  scalar map { x => if(x) 1.0 else 0.0} }
            )
          case (sourceType, targetType) =>
            throw new StratosphereExecutionError(s"Gilbert does not support type conversion from $sourceType to " +
              s"$targetType")
        }

      case typeConversion: TypeConversionMatrix =>
        (typeConversion.sourceType, typeConversion.targetType) match {
          case (MatrixType(BooleanType,_,_), MatrixType(DoubleType,_,_)) =>
            handle[TypeConversionMatrix, BooleanMatrix](
            typeConversion,
            {input => evaluate[BooleanMatrix](input.matrix)},
            {
              (_, matrix) =>
                val result = matrix map { submatrix =>
                  Submatrix(submatrix.getPartition, submatrix.activeIterator map {
                    case ((row,col), value) =>
                      (row,col, if(value) 1.0 else 0.0)
                    } toSeq
                  )
                }
                result.setName("TypeConversionMatrix")

                if(configuration.compilerHints){
                  result.uniqueKey(s => (s.rowIndex, s.columnIndex))
                }

                result
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
                case (_, (matrixDS, rowsMultDS, colsMultDS)) =>
                  val rowsColsMult = rowsMultDS cross colsMultDS map { (rowsMult, colsMult) => (rowsMult.toInt, colsMult.toInt)}
                  rowsColsMult.setName("Repmat: Pair rows and cols multiplier")

                  val newBlocks = matrixDS cross rowsColsMult flatMap { case (matrix, (rowsMult, colsMult)) =>
                    val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rowsMult*matrix.totalRows,
                      colsMult*matrix.totalColumns)
                    val rowIncrementor = matrix.totalRows
                    val colIncrementor = matrix.totalColumns


                    val result = for(rowIdx <- matrix.rowIndex until partitionPlan.maxRowIndex by rowIncrementor;
                    colIdx <- matrix.columnIndex until partitionPlan.maxColumnIndex by colIncrementor) yield{
                      val partition = partitionPlan.getPartition(rowIdx, colIdx)
                      (partition.id, partition)
                    }

                    result.toIterator
                  }
                  newBlocks.setName("Repmat: New blocks")

                  val repmatEntries = matrixDS cross rowsColsMult flatMap { case (matrix, (rowsMult, colsMult)) =>
                    val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize,
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

                  if(configuration.compilerHints){
                    rowsColsMult.outputCardinality = 1;
                    newBlocks.uniqueKey(p => p._1)
                    result.uniqueKey(s => (s.rowIndex, s.columnIndex))
                  }
                  result

              }
            )
          case MatrixType(BooleanType, _, _) =>
            handle[repmat, (BooleanMatrix, Scalar[Double], Scalar[Double])](
            r,
            {r => (evaluate[BooleanMatrix](r.matrix), evaluate[Scalar[Double]](r.numRows),
              evaluate[Scalar[Double]](r.numCols))},
            {
              case (_, (matrixDS, rowsMultDS, colsMultDS)) =>
                val rowsColsMult = rowsMultDS cross colsMultDS map { (rowsMult, colsMult) => (rowsMult.toInt, colsMult.toInt)}
                rowsColsMult.setName("Repmat: Pair rows and cols multiplier")

                val newBlocks = matrixDS cross rowsColsMult flatMap { case (matrix, (rowsMult, colsMult)) =>
                  val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rowsMult*matrix.totalRows,
                    colsMult*matrix.totalColumns)
                  val rowIncrementor = matrix.totalRows
                  val colIncrementor = matrix.totalColumns


                  val result = for(rowIdx <- matrix.rowIndex until partitionPlan.maxRowIndex by rowIncrementor;
                                   colIdx <- matrix.columnIndex until partitionPlan.maxColumnIndex by colIncrementor) yield{
                    val partition = partitionPlan.getPartition(rowIdx, colIdx)
                    (partition.id, partition)
                  }

                  result.toIterator
                }

                newBlocks.setName("Repmat: New blocks")

                val repmatEntries = matrixDS cross rowsColsMult flatMap { case (matrix, (rowsMult, colsMult)) =>
                  val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize,
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

                    BooleanSubmatrix(partition, (entries map { case (id, row, col, value) => (row, col, value)}).toSeq)
                }
                result.setName("Repmat: Repeated matrix")

                if(configuration.compilerHints){
                  rowsColsMult.outputCardinality = 1;
                  newBlocks.uniqueKey(p => p._1)
                  result.uniqueKey(s => (s.rowIndex, s.columnIndex))
                }
                result

            }
            )
        }

      case l: linspace =>
        handle[linspace, (Scalar[Double], Scalar[Double], Scalar[Double])](
        l,
        { l => (evaluate[Scalar[Double]](l.start), evaluate[Scalar[Double]](l.end),
          evaluate[Scalar[Double]](l.numPoints))},
        { case (_,(startDS, endDS, numPointsDS)) =>
          val startEnd = startDS cross endDS map { (start, end) => (start, end)}
          startEnd.setName("Linspace: Start end pair")

          val blocks = numPointsDS flatMap { num =>
            val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, 1, num.toInt)
            for(partition <- partitionPlan.iterator) yield partition
          }
          blocks.setName("Linspace: New blocks")

          val result = blocks cross startEnd map { case (block, (start, end)) =>
            val spacing = (end-start)/(block.numTotalColumns-1)
            val result = Submatrix(block, block.numColumns)

            val entries = for(col <- result.colRange) yield {
              result.update(0,col, spacing*col + start)
            }
            result
          }
          result.setName("Linspace: Linear spaced matrix")

          if(configuration.compilerHints){
            startEnd.outputCardinality = 1
            blocks.uniqueKey(p => p.id)
            result.uniqueKey(s => (s.rowIndex, s.columnIndex))
          }

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
                  val minPerColumn = for(col <- matrix.colRange) yield{
                    val (minRow, minValue) = matrix(::, col).iterator.minBy{ case (row, value) => value }
                    (col , minRow, minValue)
                  }
                  minPerColumn.toIterator
                }else if(dim == 2){
                  val minPerRow = for(row <- matrix.rowRange) yield {
                    val ((_,minCol), minValue) = matrix(row, ::).iterator.minBy { case (col, value) => value }
                    (row, minCol, minValue)
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
              minIndexValue.setName("MinWithIndex: argmin and min scalarRef")

              val newBlocks = totalSizeDimension flatMap { case (rows, cols, _) =>
                val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rows, cols)
                for(partition <- partitionPlan.toIterator) yield {
                  (partition.id, partition)
                }
              }
              newBlocks.setName("MinWithIndex: New blocks")

              val partitionedMinIndexValue = minIndexValue cross totalSizeDimension map { (mIdxValue, sizeDimension) =>
                val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, sizeDimension._1,
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
              partitionedMinIndexValue.setName("MinWithIndex: Partitioned argmin and min scalarRef")

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

              minValues.union(minIndices)
              val result = minValues union minIndices
              result.setName("MinWithIndex: Cell array")

              if(configuration.compilerHints){
                newBlocks.uniqueKey(p => p._1)
              }

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
                  val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, a.totalRows, b.totalRows)
                  val newEntries = for(rowA <- a.rowRange; rowB <- b.rowRange) yield {
                    val diff = a(rowA,::) - b(rowB,::)
                    val diffsq = diff :^ 2.0
                    val summedDiff = _root_.breeze.linalg.sum(diffsq)
                    (rowA, rowB, summedDiff)
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
                  summedDiffs :^ 0.5
              }
              pdist2.setName("Pdist2: pair wise distance matrix")

              if(configuration.compilerHints){
                pdist2.uniqueKey(s => (s.rowIndex, s.columnIndex))
              }
              pdist2
          }
        )
    }
  }
}

object StratosphereExecutor{
  val log = LogFactory.getLog(classOf[StratosphereExecutor])
}