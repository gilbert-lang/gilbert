package org.gilbertlang.runtime.execution.flink

import _root_.breeze.linalg.{*, max, min, norm}
import _root_.breeze.stats.distributions.{Gaussian, Uniform}
import org.apache.commons.logging.LogFactory
import org.apache.flink.api.java.operators.DataSink
import org.gilbertlang.runtime.Executor
import org.apache.flink.api.scala._
import org.gilbertlang.runtimeMacros.linalg.operators.{SubmatrixImplicits, SubvectorImplicits}

import scala.collection.convert.WrapAsScala
import org.gilbertlang.runtime.Operations._
import org.gilbertlang.runtime.Executables._
import org.gilbertlang.runtimeMacros.linalg._

import scala.language.implicitConversions
import scala.language.reflectiveCalls
import org.gilbertlang.runtime.RuntimeTypes._
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
class FlinkExecutor(@transient val env: ExecutionEnvironment, val appName: String)
    extends Executor
    with WrapAsScala
    with SubmatrixImplicits
    with SubvectorImplicits  {

  import ImplicitConversions._

  type Matrix = DataSet[Submatrix]
  type BooleanMatrix = DataSet[BooleanSubmatrix]
  type Scalar[T] = DataSet[T]
  type CellArray = DataSet[CellEntry]
  private var tempFileCounter = 0
  @transient private var iterationStatePlaceholderValue: Option[Matrix] = None
  @transient private var iterationStatePlaceholderValueCellArray: Option[CellArray] = None
  @transient private var convergenceCurrentStateValue: Option[Matrix] = None
  @transient private var convergenceNextStateValue: Option[Matrix] = None
  @transient private var convergenceCurrentStateCellArrayValue: Option[CellArray] = None
  @transient private var convergencePreviousStateCellArrayValue: Option[CellArray] = None

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
              List(matrix
                .map{
                  matrix =>
                    Submatrix.outputFormatter(matrix, "\n", " ", configuration.verboseWrite)
                }
                .writeAsText(completePathWithFilename)
                .name(s"WriteMatrix($completePathWithFilename)"))
            }
            })
          case MatrixType(BooleanType,_,_) =>
            handle[WriteMatrix, BooleanMatrix](
            executable,
            { exec => evaluate[BooleanMatrix](exec.matrix) },
            { (_, matrix) =>
            {
              val completePathWithFilename = newTempFileName()
              List(matrix.map(BooleanSubmatrix.outputFormatter("\n", " ", configuration.verboseWrite))
                .writeAsText(completePathWithFilename)
                .name(s"WriteMatrix($completePathWithFilename)"))
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
          val result = new Array[DataSink[_]](cellArrayType.elementTypes.length)
          while(index < cellArrayType.elementTypes.length){
            val completePathWithFilename = newTempFileName()
            val loopIndex = index
            val filtered = cellArray filter {
              x =>
                x.index == loopIndex
            }
            filtered.name("WriteCellArray: Select entry")

            val sink: DataSink[_] = cellArrayType.elementTypes(index) match {
              case MatrixType(DoubleType,_,_) =>
                val mappedCell = filtered map {
                  x =>
                    x.wrappedValue[Submatrix]
                }
                mappedCell.name("WriteCellArray: Unwrapped scalarRef Matrix(Double)")
                mappedCell.map{
                  submatrix =>
                    Submatrix.outputFormatter(submatrix, "\n", " ", configuration.verboseWrite)
                }
                  .writeAsText(completePathWithFilename)
                  .name(s"WriteCellArray(Matrix[Double], $completePathWithFilename)")
              case StringType =>
                val mappedCell =filtered map( x => x.wrappedValue[String])
                mappedCell.name("WriteCellArray: Unwrapped scalarRef String")
                mappedCell.writeAsCsv(completePathWithFilename).name(s"WriteCellArray(String, $completePathWithFilename)")
              case DoubleType =>
                val mappedCell = filtered map(x => x.wrappedValue[Double])
                mappedCell.name("WriteCellArray: Unwrapped scalarRef Double")
                mappedCell.writeAsCsv(completePathWithFilename).name(s"WriteCellArray(Double, $completePathWithFilename)")
              case BooleanType =>
                val mappedCell = filtered map(x => x.wrappedValue[Boolean])
                mappedCell.name("WriteCellArray: Unwrapped scalarRef Boolean")
                mappedCell.writeAsCsv(completePathWithFilename).name(s"WriteCellArray(Boolean, $completePathWithFilename)")
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
              List(string.writeAsCsv(completePathWithFilename).name(s"WriteString($completePathWithFilename)"))
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
              List(scalar.writeAsCsv(completePathWithFilename).name(s"WriteScalarRef($completePathWithFilename)"))
            }
            })
          case BooleanType =>
            handle[WriteScalar, Scalar[Boolean]](
            executable,
            { exec => evaluate[Scalar[Boolean]](exec.scalar) },
            { (_, scalar) =>
              val completePathWithFilename = newTempFileName()
              List(scalar.writeAsCsv(completePathWithFilename).name(s"WriteScalarRef($completePathWithFilename)"))
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
                      val result = scalar cross matrix apply {(scalar, submatrix) => submatrix :& scalar}
                      result.name("SM: Logical And")
                      result
                    case Or | SCOr =>
                      val result = scalar cross matrix apply { (scalar, submatrix) => submatrix :| scalar }
                      result.name("SM: Logical Or")
                      result
                  }
                  if(configuration.compilerHints) {
                    if (configuration.preserveHint) {
                      result.withForwardedFieldsSecond("rowIndex -> rowIndex", "columnIndex -> columnIndex", "rowOffset -> rowOffset", "columnOffset -> columnOffset", "totalRows -> totalRows", "totalColumns -> totalColumns")
                    }
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
                  val result = scalarDS cross matrixDS apply { (scalar, submatrix) => submatrix + scalar }
                  result.name("SM: Addition")
                  result
                case Subtraction =>
                  val result = scalarDS cross matrixDS apply { (scalar, submatrix) => submatrix + -scalar }
                  result.name("SM: Subtraction")
                  result
                case Multiplication =>
                  val result = scalarDS cross matrixDS apply { (scalar, submatrix) => submatrix * scalar }
                  result.name("SM: Multiplication")
                  result
                case Division =>
                  val result = scalarDS cross matrixDS apply { (scalar, submatrix) =>
                    {
                      val partition = submatrix.getPartition
                      val result = Submatrix.init(partition, scalar)
                      result / submatrix
                    }
                  }
                  result.name("SM: Division")
                  result
                case Exponentiation =>
                  val result = scalarDS cross matrixDS apply { (scalar, submatrix) =>
                    val partition = submatrix.getPartition
                    val scalarMatrix = Submatrix.init(partition, scalar)
                    scalarMatrix :^ submatrix
                  }
                  result.name("SM: CellwiseExponentiation")
                  result
              }
              if(configuration.compilerHints) {
                if(configuration.preserveHint) {
                  result.withForwardedFieldsSecond("rowIndex -> rowIndex", "columnIndex -> columnIndex", "rowOffset -> rowOffset", "columnOffset -> columnOffset", "totalRows -> totalRows", "totalColumns -> totalColumns")
                }
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
                    val result = scalar cross matrix apply { (scalar, submatrix) => submatrix :< scalar }
                    result.name("SM: Greater than")
                    result
                  case GreaterEqualThan =>
                    val result = scalar cross matrix apply { (scalar, submatrix) => submatrix :<= scalar }
                    result.name("SM: Greater equal than")
                    result
                  case LessThan =>
                    val result = scalar cross matrix apply { (scalar, submatrix) => submatrix :> scalar }
                    result.name("SM: Less than")
                    result
                  case LessEqualThan =>
                    val result = scalar cross matrix apply { (scalar, submatrix) => submatrix :>= scalar }
                    result.name("SM: Less equal than")
                    result
                  case Equals =>
                    val result = scalar cross matrix apply { (scalar, submatrix) => submatrix :== scalar }
                    result.name("SM: Equals")
                    result
                  case NotEquals =>
                    val result = scalar cross matrix apply { (scalar, submatrix) => submatrix :!= scalar }
                    result.name("SM: Not equals")
                    result
                }
                if(configuration.compilerHints) {
                  if(configuration.preserveHint){
                    result.withForwardedFieldsSecond("rowIndex -> rowIndex", "columnIndex -> columnIndex", "rowOffset -> rowOffset", "columnOffset -> columnOffset", "totalRows -> totalRows", "totalColumns -> totalColumns")
                  }
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
                      val result = matrix cross scalar apply { (submatrix, scalar) => submatrix :& scalar }
                      result.name("MS: Logical And")
                      result
                    case Or | SCOr =>
                      val result = matrix cross scalar apply { (submatrix, scalar) => submatrix :| scalar }
                      result.name("MS: Logical Or")
                      result
                  }
                  if(configuration.compilerHints) {
                    if(configuration.preserveHint){
                      result.withForwardedFieldsFirst("rowIndex -> rowIndex", "columnIndex -> columnIndex", "rowOffset -> rowOffset", "columnOffset -> columnOffset", "totalRows -> totalRows", "totalColumns -> totalColumns")
                    }
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
                  val result = matrixDS cross scalarDS apply { (submatrix, scalar) => submatrix + scalar }
                  result.name("MS: Addition")
                  result
                case Subtraction =>
                  val result = matrixDS cross scalarDS apply { (submatrix, scalar) => submatrix - scalar }
                  result.name("MS: Subtraction")
                  result
                case Multiplication =>
                  val result = matrixDS cross scalarDS apply { (submatrix, scalar) => submatrix * scalar }
                  result.name("MS: Multiplication")
                  result
                case Division =>
                  val result = matrixDS cross scalarDS apply { (submatrix, scalar) => submatrix / scalar }
                  result.name("MS: Division")
                  result
                case Exponentiation =>
                  val result = matrixDS cross scalarDS apply { (submatrix, scalar) => submatrix :^ scalar}
                  result.name("MS: Exponentiation")
                  result
              }
              if(configuration.compilerHints) {
                if(configuration.preserveHint){
                  result.withForwardedFieldsFirst("rowIndex -> rowIndex", "columnIndex -> columnIndex", "rowOffset -> rowOffset", "columnOffset -> columnOffset", "totalRows -> totalRows", "totalColumns -> totalColumns")
                }
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
                    val result = matrix cross scalar apply { (submatrix, scalar) => submatrix :> scalar }
                    result.name("MS: Greater than")
                    result
                  case GreaterEqualThan =>
                    val result = matrix cross scalar apply { (submatrix, scalar) => submatrix :>= scalar }
                    result.name("MS: Greater equal than")
                    result
                  case LessThan =>
                    val result = matrix cross scalar apply { (submatrix, scalar) => submatrix :< scalar }
                    result.name("MS: Less than")
                    result
                  case LessEqualThan =>
                    val result = matrix cross scalar apply { (submatrix, scalar) => submatrix :<= scalar }
                    result.name("MS: Less equal than")
                    result
                  case Equals =>
                    val result = matrix cross scalar apply { (submatrix, scalar) => submatrix :== scalar }
                    result.name("MS: Equals")
                    result
                  case NotEquals =>
                    val result = matrix cross scalar apply { (submatrix, scalar) => submatrix :!= scalar }
                    result.name("MS: Not equals")
                    result
                }
                if(configuration.compilerHints){
                  if(configuration.preserveHint){
                    result.withForwardedFieldsFirst("rowIndex -> rowIndex", "columnIndex -> columnIndex", "rowOffset -> rowOffset", "columnOffset -> columnOffset", "totalRows -> totalRows", "totalColumns -> totalColumns")
                  }
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
                    val result = left cross right apply { (left, right) => left & right }
                    result.name("SS: Logical And")
                    result
                  case Or =>
                    val result = left cross right apply { (left, right) => left | right }
                    result.name("SS: Logical Or")
                    result
                  case SCAnd  =>
                    val result = left cross right apply { (left, right) => left && right }
                    result.name("SS: Logical And")
                    result
                  case SCOr =>
                    val result = left cross right apply { (left, right) => left || right }
                    result.name("SS: Logical Or")
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
                      val result = left cross right apply { (left, right) => left + right }
                      result.name("SS: Addition")
                      result
                    case Subtraction =>
                      val result = left cross right apply { (left, right) => left - right }
                      result.name("SS: Subtraction")
                      result
                    case Multiplication =>
                      val result = left cross right apply { (left, right) => left * right }
                      result.name("SS: Multiplication")
                      result
                    case Division =>
                      val result =left cross right apply { (left, right) => left / right }
                      result.name("SS: Division")
                      result
                    case Exponentiation =>
                      val result = left cross right apply { (left, right) => math.pow(left,right)}
                      result.name("SS: Exponentiation")
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
                    val result = left union right reduce { (a, b) => max(a, b) }
                    result.name("SS: Maximum")
                    result
                  case Minimum =>
                    val result = left union right reduce { (a, b) => min(a, b) }
                    result.name("SS: Minimum")
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
                    val result = left cross right apply { (left, right) => left > right }
                    result.name("SS: Greater than")
                    result
                  case GreaterEqualThan =>
                    val result = left cross right apply { (left, right) => left >= right }
                    result.name("SS: Greater equal than")
                    result
                  case LessThan =>
                    val result = left cross right apply { (left, right) => left < right }
                    result.name("SS: Less than")
                    result
                  case LessEqualThan =>
                    val result = left cross right apply { (left, right) => left <= right }
                    result.name("SS: Less equal than")
                    result
                  case Equals =>
                    val result = left cross right apply { (left, right) => left == right}
                    result.name("SS: Equals")
                    result
                  case NotEquals =>
                    val result = left cross right apply { (left, right) => left != right }
                    result.name("SS: Not equals")
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
                  matrix map { x => max(x) } reduce { (a, b) => max(a, b) }
                case Minimum =>
                  matrix map { x => min(x) } reduce { (a, b) => min(a, b) }
                case Norm2 =>
                  matrix map { x => _root_.breeze.linalg.sum(x:*x) } reduce
                    { (a, b) => a + b } map
                    { x => math.sqrt(x) }
                case SumAll =>
                  val blockwiseSum = matrix map { x => _root_.breeze.linalg.sum(x) }
                  blockwiseSum.name("Aggregate Matrix: Blockwise sum.")
                  val result = blockwiseSum reduce ( (a, b) => a + b)
                  result.name("Aggregate Matrix: Sum all")
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
                  scalarDS map { binarize(_) }
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
            env.fromElements(exec.value)
          })

      case executable: boolean =>
        handle[boolean, Unit](
            executable,
            {_ => },
            { (exec, _) =>
              env.fromElements(exec.value)
            })

      case executable: string =>
        handle[string, Unit](
          executable,
          { _ => },
          { (exec, _) =>
            env.fromElements(exec.value)
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
                  result.withForwardedFields("rowIndex -> rowIndex", "columnIndex -> columnIndex",
                    "rowOffset -> rowOffset", "columnOffset -> columnOffset", "totalRows -> totalRows",
                    "totalColumns -> totalColumns")
                }
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
                      val result = left join right where { x => (x.rowIndex, x.columnIndex) } equalTo
                      { y => (y.rowIndex, y.columnIndex) } apply
                      { (left, right) => left :& right }
                      result.name("MM: Logical And")
                      result
                    case Or | SCOr =>
                      val result = left join right where { x => (x.rowIndex, x.columnIndex) } equalTo
                      { y => (y.rowIndex, y.columnIndex) } apply
                      { (left, right) => left :| right }
                      result.name("MM: Logical Or")
                      result
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
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } equalTo
                    { y => (y.rowIndex, y.columnIndex) } apply
                    { (left, right) =>
                      val result = left + right
                      result
                    }
                  result.name("MM: Addition")
                  result
                case Subtraction =>
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } equalTo
                    { y => (y.rowIndex, y.columnIndex) } apply
                    { (left, right) => left - right }
                  result.name("MM: Subtraction")
                  result
                case Multiplication =>
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } equalTo
                    { y => (y.rowIndex, y.columnIndex) } apply
                    { (left, right) => left :* right
                    }
                  result.name("MM: Cellwise multiplication")
                  result
                case Division =>
                  val result = left join right where { x => (x.rowIndex, x.columnIndex) } equalTo
                    { y => (y.rowIndex, y.columnIndex) } apply
                    { (left, right) => left / right
                    }
                  result.name("MM: Division")
                  result
                case Exponentiation =>
                  val result = left join right where {x => (x.rowIndex, x.columnIndex)} equalTo { y => (y.rowIndex,
                   y.columnIndex)} apply { (left, right) => left :^ right }
                  result.name("MM: Exponentiation")
                  result
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
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } equalTo
                      { y => (y.rowIndex, y.columnIndex) } apply
                      { (left, right) =>
                        numerics.max(left, right)
                      }
                    result.name("MM: Maximum")
                    result
                  case Minimum =>
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } equalTo
                      { y => (y.rowIndex, y.columnIndex) } apply
                      { (left, right) =>
                        numerics.min(left, right)
                      }
                    result.name("MM: Minimum")
                    result
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
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } equalTo
                      { y => (y.rowIndex, y.columnIndex) } apply
                      { (left,right) => left :> right }
                    result.name("MM: Greater than")
                    result
                  case GreaterEqualThan =>
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } equalTo
                      {y => (y.rowIndex, y.columnIndex) } apply
                      { (left, right) => left :>= right }
                    result.name("MM: Greater equal than")
                    result
                  case LessThan =>
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } equalTo
                      { y => (y.rowIndex, y.columnIndex) } apply
                      { (left, right) => left :< right }
                    result.name("MM: Less than")
                    result
                  case LessEqualThan =>
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } equalTo
                      { y => (y.rowIndex, y.columnIndex) } apply
                      { (left, right) => left :<= right }
                    result.name("MM: Less equal than")
                    result
                  case Equals =>
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } equalTo
                      { y => (y.rowIndex, y.columnIndex) } apply
                      { (left, right) => left :== right }
                    result.name("MM: Equals")
                    result
                  case NotEquals =>
                    val result = left join right where { x => (x.rowIndex, x.columnIndex) } equalTo
                      { y => (y.rowIndex, y.columnIndex) } apply
                      { (left, right) => left :!= right }
                    result.name("MM: NotEquals")
                    result
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
              val joinedBlocks = left join right where { leftElement => leftElement.columnIndex } equalTo
                { rightElement => rightElement.rowIndex } apply
                { (left, right) =>
                  FlinkExecutor.log.info("Start matrix multiplication")
                  val result = left * right
                  FlinkExecutor.log.info("End matrix multiplication")

                  result
                }

              val reduced = joinedBlocks groupBy
                { element => (element.rowIndex, element.columnIndex) } reduce (_ + _)

              if(configuration.compilerHints){
                if(configuration.preserveHint){
                  reduced.withForwardedFields("rowIndex -> rowIndex", "columnIndex -> columnIndex",
                    "rowOffset -> rowOffset", "columnOffset -> columnOffset", "totalRows -> totalRows",
                    "totalColumns -> totalColumns")
                }
              }
              reduced
          })

      case executable: Transpose =>
        handle[Transpose, Matrix](
          executable,
          { exec => evaluate[Matrix](exec.matrix) },
          { (_, matrixDS) =>
            val result = matrixDS map { matrix => matrix.t }
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
                  blockwiseNorm.name("VM: Blockwise L1 norm")

                  val l1norm = blockwiseNorm groupBy (subvector => subvector.index) reduce (_ + _)
                  l1norm.name("VM: L1 norm")

                  val normedMatrix =  l1norm join
                    matrix where { l1norm => l1norm.index } equalTo { submatrix => submatrix.rowIndex } apply
                    { (l1norm, submatrix) =>
                      val result = submatrix.copy
                      for(col <- submatrix.colRange)
                        result(::, col) :/= l1norm

                      result
                    }
                  normedMatrix.name("VM: L1 normed matrix")

                  if(configuration.compilerHints){
                    if(configuration.preserveHint){
                      l1norm.withForwardedFields("index -> index", "offset -> offset", "totalEntries -> totalEntries")

                      normedMatrix.withForwardedFieldsSecond("rowIndex -> rowIndex", "columnIndex -> columnIndex",
                      "rowOffset -> rowOffset", "columnOffset -> columnOffset", "totalRows -> totalRows",
                      "totalColumns -> totalColumns")
                    }
                  }

                  normedMatrix
                case Maximum =>
                  val blockwiseMax = matrix map { submatrix => max(submatrix(*, ::)) }
                  blockwiseMax.name("VM: Blockwise maximum")

                  val maximum = blockwiseMax groupBy
                    { subvector => subvector.index } reduce (numerics.max(_, _))
                  maximum.name("VM: vectorwise maximum")

                  val matrixResult = maximum map { subvector => subvector.asMatrix }
                  matrixResult.name("VM: vectorwise maximum matrix form")

                  if(configuration.compilerHints){
                    if(configuration.preserveHint){
                      maximum.withForwardedFields("index -> index", "offset -> offset", "totalEntries -> totalEntries")
                    }
                  }

                  matrixResult
                case Minimum =>
                  val blockwiseMin = matrix map { submatrix => min(submatrix(*, ::)) }
                  blockwiseMin.name("VM: Blockwise minimum")

                  val minimum = blockwiseMin groupBy
                    { subvector => subvector.index } reduce { numerics.min(_, _) }
                  minimum.name("VM: Vectorwise minimum")

                  val matrixResult = minimum map { subvector => subvector.asMatrix }
                  matrixResult.name("VM: Vectorwise minimum in matrix form")

                  if(configuration.compilerHints){
                    if(configuration.preserveHint){
                      minimum.withForwardedFields("index -> index", "offset -> offset", "totalEntries -> totalEntries")
                    }
                  }

                  matrixResult
                case Norm2 =>
                  val squaredValues = matrix map { submatrix => submatrix :^ 2.0 }
                  squaredValues.name("VWM: Norm2 squared values")

                  val blockwiseSum = squaredValues map { submatrix => _root_.breeze.linalg.sum(submatrix(*,
                    ::)) }
                  blockwiseSum.name("VM: Norm2 blockwise sum")

                  val sumSquaredValues = blockwiseSum groupBy
                    { subvector => subvector.index } reduce (_ + _)

                  sumSquaredValues.name("VWM: Norm2 sum squared values")

                  val result = sumSquaredValues map {
                    sqv =>
                      sqv.asMatrix mapActiveValues { value => math.sqrt(value) }
                  }
                  result.name("VWM: Norm2")

                  if(configuration.compilerHints){
                    if(configuration.preserveHint){
                      squaredValues.withForwardedFields("rowIndex -> rowIndex", "columnIndex -> columnIndex",
                      "rowOffset -> rowOffset", "columnOffset -> columnOffset", "totalRows -> totalRows",
                      "totalColumns -> totalColumns")

                      sumSquaredValues.withForwardedFields("index -> index", "offset -> offset", "totalEntries -> totalEntries")
                    }
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
              val pathLiteral = path.getValue(0)
              val completePath = "file://" + pathLiteral
              val source = env.readCsvFile[(Int, Int, Double)](completePath, "\n", " ")

              val rowsCols = rows cross cols
              val rowsColsPair = rowsCols apply {
                (rows, cols) => (rows.toInt, cols.toInt)
              }
              rowsColsPair.name("LoadMatrix: Rows and Cols pair")

              val blocks = rowsColsPair flatMap {  pair =>
                val (numRows, numCols) = pair
                val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, numRows, numCols)
                for (partition <- partitionPlan.iterator) yield {
                  (partition.id, partition)
                }
              }
              blocks.name("LoadMatrix: Partition blocks")

              val partitionedData = rowsColsPair cross source apply {
                (left, right) =>
                  val ((numRows, numCols), (row, column, value)) = (left, right)
                  val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, numRows, numCols)
                  (partitionPlan.partitionId(row-1, column-1), row-1, column-1, value)
              }
              partitionedData.name("LoadMatrix: Partitioned data")

              val loadedMatrix = partitionedData coGroup blocks where { entry => entry._1 } equalTo { block => block
              ._1 } apply {
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
              loadedMatrix.name("LoadMatrix: Loaded matrix")

              loadedMatrix
          })

      case compound: CompoundExecutable =>
        compound.executables flatMap { x => evaluate[List[DataSink[_]]](x) }

      case executable: ones =>
        handle[ones, (Scalar[Double], Scalar[Double])](
          executable,
          { exec => (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numColumns)) },
          {
            case (_, (rows, columns)) =>
              val partitions = rows cross columns flatMap { pair =>
                val (rows, columns) = pair
                val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rows.toInt, columns.toInt)
                partitionPlan.iterator
              }
              partitions.name("Ones: Partitions")

              //reduceGroup to distributed the partitions across the worker nodes
              val distributedPartitions = partitions.groupBy{p => p.id}.reduceGroup{ps => ps.next}
              distributedPartitions.name("Ones: Distributed partitions")

              val result = distributedPartitions map { p =>
                Submatrix.init(p, 1.0)
              }

              result.name("Ones")

              result
          })

      case executable: zeros =>
        handle[zeros, (Scalar[Double], Scalar[Double])](
            executable,
            { exec => (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numCols))},
            { case (_, (rows, columns)) =>
              val partitions = rows cross columns flatMap { pair =>
                val (rows, columns) = pair
                val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rows.toInt, columns.toInt)

                partitionPlan.iterator
              }
              partitions.name("Zeros: Partitions")

              //reduceGroup to distribute the partitions across the worker nodes
              val distributedPartitions = partitions.groupBy(p => p.id).reduceGroup(ps => ps.next)
              distributedPartitions.name("Zeros: Distributed partitions")

              val result = distributedPartitions map { p => Submatrix(p)}

              result.name(s"Zeros")

              result
            })

      case executable: eye =>
        handle[eye, (Scalar[Double], Scalar[Double])](
            executable,
            { exec => (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numCols))},
            { case (_, (rows, columns)) =>
              val partitions = rows cross columns flatMap { pair =>
                val (rows, columns) = pair
                val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rows.toInt, columns.toInt)
                partitionPlan.iterator
              }
              partitions.name("Eye: Partitions")

              //reduceGroup to distribute partitions across worker nodes
              val distributedPartitions = partitions.groupBy{p => p.id}.reduceGroup{
                ps =>
                  val result = ps.next
                  val s = ps.size
                  result
              }
              distributedPartitions.name("Eye: Distributed partitions")

              val result = distributedPartitions.map{
                p =>
                  Submatrix.eye(p)
              }

              result.name("Eye")

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
                pair =>
                  val (rows, cols) = pair
                  val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rows.toInt,
                    cols.toInt)
                  partitionPlan.iterator
              }
              partitions.name("Randn: Partitions")

              //reduceGroup to distribute the partitions across the worker nodes
              val distributedPartitions = partitions.groupBy{p => p.id}.reduceGroup{ ps => ps.next}
              distributedPartitions.name("Randn: Distributed partitions")

              val meanStd = meanDS cross stdDS apply { (mean, std) => (mean, std) }
              meanStd.name("Randn: mean and std pair")

              val randomBlocks = distributedPartitions cross meanStd apply {
                (partition, meanStd) =>
                  Submatrix.rand(partition, Gaussian(meanStd._1, meanStd._2))

              }

              randomBlocks.name("Randn: Random submatrices")

              randomBlocks
          })

      case executable: urand =>
        handle[urand, (Scalar[Double], Scalar[Double])](
        executable,
        { exec =>
          (evaluate[Scalar[Double]](exec.numRows), evaluate[Scalar[Double]](exec.numColumns))
        },
        {
          case (_, (rowsDS, colsDS)) =>
            val partitions = rowsDS cross colsDS flatMap {
              pair =>
                val (rows, cols) = pair
                val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rows.toInt,
                  cols.toInt)
                partitionPlan.iterator
            }
            partitions.name("URand: Partitions")

            //reduceGroup to distribute the partitions across the worker nodes
            val distributedPartitions = partitions.groupBy{p => p.id}.reduceGroup{ ps => ps.next}
            distributedPartitions.name("URand: Distributed partitions")

            val randomBlocks = distributedPartitions map {
              partition =>
                val uniform = new Uniform(0.0, 1.0)
                Submatrix.rand(partition, uniform)

            }

            randomBlocks.name("URand: Random submatrices")

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
            val partitions = rowsDS cross colsDS flatMap { pair =>
              val (rows, cols) = pair
              val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rows.toInt, cols.toInt)

              partitionPlan.iterator
            }
            partitions.name("Sprand: Random partitions")

            //reduceGroup to distribute the partitions across the worker nodes
            val distributedPartitions = partitions.groupBy(p => p.id).reduceGroup(ps => ps.next)
            distributedPartitions.name("Sprand: Disitributed partitions")

            val meanStd = meanDS cross stdDS apply { (mean, std) => (mean, std)}
            meanStd.name("Sprand: mean and std combined")

            val meanStdLevel = meanStd cross stdDS apply { (meanStd, level) => (meanStd._1, meanStd._2, level)}
            meanStdLevel.name("Sprand: Mean, std and level combined")

            val randomSparseBlocks = distributedPartitions cross meanStdLevel apply {
              (partition, meanStdLevel) =>
                val rand = Gaussian(meanStdLevel._1, meanStdLevel._2)
                Submatrix.sprand(partition, rand, meanStdLevel._3)
            }

            randomSparseBlocks.name("Sprand: Sparse random blocks")

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
            val partitions = rowsDS cross colsDS flatMap { pair =>
              val (rows, cols) = pair
              val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rows.toInt, cols.toInt)
              partitionPlan.iterator
            }
            partitions.name("AdaptiveRand: Partitions")

            //reduceGroup to distribute the partitions explicitely across the worker nodes
            val distributedPartitions = partitions.groupBy(p => p.id).reduceGroup(ps => ps.next)
            distributedPartitions.name("AdaptiveRand: Distributed partitions")

            val meanStd = meanDS cross stdDS apply {(mean, std) => (mean, std)}
            meanStd.name("AdaptiveRand: Mean and std pair")

            val meanStdLevel = meanStd cross levelDS apply { (meanStd, level) => (meanStd._1, meanStd._2, level)}
            meanStdLevel.name("AdaptiveRand: Mean, std and level triple")

            val result = distributedPartitions cross meanStdLevel apply {
              (partition, meanStdLevel) =>
                val random = Gaussian(meanStdLevel._1, meanStdLevel._2)
                Submatrix.adaptiveRand(partition, random, meanStdLevel._3, configuration.densityThreshold)
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

              result.name("Spones")

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
              blockwiseSum.name("SumRow: Blockwise rowwise sum")

              val rowwiseSum = blockwiseSum groupBy
                { subvector => subvector.index } reduce (_ + _)
              rowwiseSum.name("SumRow: Row-wise sum")

              val matrixResult = rowwiseSum map { subvector => subvector.asMatrix }
              matrixResult.name("SumRow: Row-wise sum in matrix form")

              if(configuration.compilerHints){
                rowwiseSum.withForwardedFields("index -> index", "offset -> offset", "totalEntries -> totalEntries")
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
              blockwiseSum.name("SumCol: Blockwise column sum")

              val colwiseSum = blockwiseSum groupBy
                { submatrix => submatrix.columnIndex } reduce (_ + _)
              colwiseSum.name("SumCol: Column sum")

              if(configuration.compilerHints){
                if(configuration.preserveHint){
                  blockwiseSum.withForwardedFields("columnIndex -> columnIndex", "columnOffset -> columnOffset", "totalColumns -> totalColumns")

                  colwiseSum.withForwardedFields("rowIndex -> rowIndex", "columnIndex -> columnIndex", "rowOffset -> rowOffset",
                  "columnOffset -> columnOffset", "totalRows -> totalRows", "totalColumns -> totalColumns")
                }
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
                  entries.name("Diag: Row vector input")

                  val result = entries cross matrix map {
                    input =>
                      val ((rowIndex, numRows, rowOffset), submatrix) = input
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
                  result.name("Diag: Result diagonal matrix of a row vector")

                  result
                case (_, Some(1)) =>
                  val entries = matrix map {
                    submatrix =>
                      (submatrix.rowIndex, submatrix.rows, submatrix.rowOffset)
                  }
                  entries.name("Diag: Column vector input")

                  val result = entries cross matrix map {
                    input =>
                      val ((columnIndex, numColumns, columnOffset), submatrix) = input
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
                  result.name("Diag: Result diagonal matrix of a column vector")

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
                  partialDiagResults.name("Diag: Extract diagonal")

                  val result = partialDiagResults groupBy { partialResult => partialResult.rowIndex } reduce (_ + _)
                  result.name("Diag: ")

                  result
              }
            }
          })

      case executable: FixpointIterationMatrix =>
        handle[FixpointIterationMatrix, (Matrix, Scalar[Double])](
          executable,
          { exec => (evaluate[Matrix](exec.initialState), evaluate[Scalar[Double]](exec.maxIterations)) },
          { case (exec, (initialState, maxIterations)) =>
            val numberIterations = maxIterations.getValue(0).toInt
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
              val terminationFunction = (current: Matrix, next: Matrix) => {
                val oldPreviousState = convergenceCurrentStateValue
                val oldCurrentState = convergenceNextStateValue
                convergenceCurrentStateValue = Some(current)
                convergenceNextStateValue = Some(next)
                val result = evaluate[Scalar[Boolean]](exec.convergencePlan)

                convergenceCurrentStateValue = oldPreviousState
                convergenceNextStateValue = oldCurrentState
                result filter { b => !b}
              }

              iteration = initialState.iterateWithTermination(numberIterations){
                current =>
                  val next = stepFunction(current)
                  val termination = terminationFunction(current, next)

                  (next, termination)
              }
            }else{
              iteration = initialState.iterate(numberIterations)(stepFunction)
            }

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
          val numberIterations = maxIterations.getValue(0).toInt
          def stepFunction(partialSolution: CellArray) = {
            val oldStatePlaceholderValue = iterationStatePlaceholderValueCellArray
            iterationStatePlaceholderValueCellArray = Some(partialSolution)
            val result = evaluate[CellArray](exec.updatePlan)
            iterationStatePlaceholderValueCellArray = oldStatePlaceholderValue
            result map { x => x }
          }

          var iteration: CellArray = null

          if(exec.convergencePlan != null){
            val terminationFunction = (current: CellArray, next: CellArray) => {
              val oldCurrentState = convergencePreviousStateCellArrayValue
              val oldNextState = convergenceCurrentStateCellArrayValue
              convergencePreviousStateCellArrayValue = Some(current)
              convergenceCurrentStateCellArrayValue = Some(next)

              val result = evaluate[Scalar[Boolean]](exec.convergencePlan)

              convergencePreviousStateCellArrayValue = oldCurrentState
              convergenceCurrentStateCellArrayValue = oldNextState

              result filter { b => !b }
            }

            iteration = initialState.iterateWithTermination(numberIterations){
              current =>
                val next = stepFunction(current)
                val termination = terminationFunction(current, next)

                (next, termination)
            }
          }else{
            iteration = initialState.iterate(numberIterations)(stepFunction)
          }

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
              val partialSum = scalar cross matrix apply { (scalar, submatrix) =>
                if (scalar == 1) {
                  (submatrix.columnIndex, _root_.breeze.linalg.sum(submatrix(::, *)))
                } else {
                  (submatrix.rowIndex, _root_.breeze.linalg.sum(submatrix(*, ::)).asMatrix)
                }
              }
              partialSum.name("Sum: Partial sum")

              val pairIDSubmatrix = partialSum groupBy { input => input._1 } reduce {
                (left, right) =>
                  (left._1, left._2 + right._2)
              }

              pairIDSubmatrix.name("Sum: (ID, sum)")

              val result = pairIDSubmatrix map (_._2)

              result.name("Sum: Final result")

              if(configuration.compilerHints){
                if(configuration.preserveHint){
                  pairIDSubmatrix.withForwardedFields("_1 -> _1")
                }
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
                result.name(s"CellEntry($index)")
                result
            }

            val firstEntry = cellArrayEntries.head
            val result = cellArrayEntries.tail.foldLeft(firstEntry)(_ union _)
            result.name("CellArray")

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
        convergenceCurrentStateValue match {
          case Some(matrix) => matrix
          case None => throw new StratosphereExecutionError("Convergence previous state scalarRef has not been set.")
        }

      case ConvergenceCurrentStatePlaceholder =>
        convergenceNextStateValue match {
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
                result.name("TypeConversionMatrix")

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
                  val rowsColsMult = rowsMultDS cross colsMultDS apply { (rowsMult, colsMult) => (rowsMult.toInt, colsMult.toInt)}
                  rowsColsMult.name("Repmat: Pair rows and cols multiplier")

                  val newBlocks = matrixDS cross rowsColsMult flatMap { pair =>
                    val (matrix, (rowsMult, colsMult)) = pair
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
                  newBlocks.name("Repmat: New blocks")

                  val repmatEntries = matrixDS cross rowsColsMult flatMap { pair =>
                    val (matrix, (rowsMult, colsMult)) = pair
                    val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize,
                      rowsMult*matrix.totalRows, colsMult*matrix.totalColumns)

                    matrix.activeIterator flatMap { case ((row, col), value) =>
                      for(rMult <- 0 until rowsMult; cMult <- 0 until colsMult) yield {
                        (partitionPlan.partitionId(rMult*matrix.totalRows + row, cMult*matrix.totalColumns + col),
                          rMult*matrix.totalRows + row, cMult*matrix.totalColumns + col, value)
                      }
                    }
                  }
                  repmatEntries.name("Repmat: Repeated entries")

                  val result = newBlocks coGroup repmatEntries where { block => block._1} equalTo { entry => entry
                    ._1} apply {
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
                  result.name("Repmat: Repeated matrix")

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
                val rowsColsMult = rowsMultDS cross colsMultDS apply { (rowsMult, colsMult) => (rowsMult.toInt, colsMult.toInt)}
                rowsColsMult.name("Repmat: Pair rows and cols multiplier")

                val newBlocks = matrixDS cross rowsColsMult flatMap { pair =>
                  val (matrix, (rowsMult, colsMult)) = pair
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

                newBlocks.name("Repmat: New blocks")

                val repmatEntries = matrixDS cross rowsColsMult flatMap { pair =>
                  val (matrix, (rowsMult, colsMult)) = pair
                  val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize,
                    rowsMult*matrix.totalRows, colsMult*matrix.totalColumns)

                  matrix.activeIterator flatMap { case ((row, col), value) =>
                    for(rMult <- 0 until rowsMult; cMult <- 0 until colsMult) yield {
                      (partitionPlan.partitionId(rMult*matrix.totalRows + row, cMult*matrix.totalColumns + col),
                        rMult*matrix.totalRows + row, cMult*matrix.totalColumns + col, value)
                    }
                  }
                }
                repmatEntries.name("Repmat: Repeated entries")

                val result = newBlocks coGroup repmatEntries where { block => block._1} equalTo { entry => entry
                  ._1} apply {
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
                result.name("Repmat: Repeated matrix")

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
          val startEnd = startDS cross endDS apply { (start, end) => (start, end)}
          startEnd.name("Linspace: Start end pair")

          val blocks = numPointsDS flatMap { num =>
            val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, 1, num.toInt)
            for(partition <- partitionPlan.iterator) yield partition
          }
          blocks.name("Linspace: New blocks")

          val result = blocks cross startEnd apply { (left, right) =>
            val (block, (start, end)) = (left, right)
            val spacing = (end-start)/(block.numTotalColumns-1)
            val result = Submatrix(block, block.numColumns)

            val entries = for(col <- result.colRange) yield {
              result.update(0,col, spacing*col + start)
            }
            result
          }
          result.name("Linspace: Linear spaced matrix")

          result
        }
        )

      case m: minWithIndex =>
        handle[minWithIndex, (Matrix, Scalar[Double])](
          m,
          {m => (evaluate[Matrix](m.matrix), evaluate[Scalar[Double]](m.dimension))},
          {
            case (_, (matrix, dimension)) =>
              val totalSizeDimension = matrix cross dimension apply { (matrix, dim) =>
                if(dim == 1){
                  (1, matrix.totalColumns, dim)
                }else if(dim == 2){
                  (matrix.totalRows, 1, dim)
                }else{
                  throw new StratosphereExecutionError("minWithIndex does not support the dimension " + dim)
                }
              } reduceGroup { entries =>
                if(entries.hasNext)
                  entries.next()
                else{
                  throw new StratosphereExecutionError("minWithIndex result matrix has to have size distinct from (0," +
                    "0)")
                }
              }
              totalSizeDimension.name("MinWithIndex: Total size with dimension")


              val minPerBlock = matrix cross dimension flatMap { pair =>
                val (matrix, dim) = pair

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
              minPerBlock.name("MinWithIndex: Min per block")

              val minIndexValue = (minPerBlock groupBy { entry => entry._1}).reduceGroup{ entries =>
                entries minBy ( x => x._3)
              }
              minIndexValue.name("MinWithIndex: argmin and min scalarRef")

              val newBlocks = totalSizeDimension flatMap { input =>
                val (rows, cols, _) = input
                val partitionPlan = new SquareBlockPartitionPlan(configuration.blocksize, rows, cols)
                for(partition <- partitionPlan.toIterator) yield {
                  (partition.id, partition)
                }
              }
              newBlocks.name("MinWithIndex: New blocks")

              val partitionedMinIndexValue = minIndexValue cross totalSizeDimension apply { (mIdxValue, sizeDimension) =>
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
              partitionedMinIndexValue.name("MinWithIndex: Partitioned argmin and min scalarRef")

              val minValues = newBlocks coGroup partitionedMinIndexValue where (x => x._1) equalTo (x => x._1) apply {
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
              minValues.name("MinWithIndex: Min values cell entry")

              val minIndices = newBlocks coGroup partitionedMinIndexValue where ( x => x._1) equalTo ( x => x._1) apply
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
              minIndices.name("MinWithIndex: Min indices cell entry")

              minValues.union(minIndices)
              val result = minValues union minIndices
              result.name("MinWithIndex: Cell array")

              result
          }
        )

      case p: pdist2 =>
        handle[pdist2, (Matrix, Matrix)](
          p,
          {p => (evaluate[Matrix](p.matrixA), evaluate[Matrix](p.matrixB)) },
          {
            case (_,(matrixA, matrixB)) =>
              val partialSqDiffs = matrixA join matrixB where { a => a.columnIndex } equalTo
                { b => b.columnIndex } apply {
                (a, b) =>
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
              partialSqDiffs.name("Pdist2: Partial squared diffs")

              val pdist2 = partialSqDiffs groupBy( x => (x.rowIndex, x.columnIndex)) reduce (_ + _) map (_ :^ 0.5)
              pdist2.name("Pdist2: pair wise distance matrix")

              pdist2
          }
        )
    }
  }
}

object FlinkExecutor{
  val log = LogFactory.getLog(classOf[FlinkExecutor])
}