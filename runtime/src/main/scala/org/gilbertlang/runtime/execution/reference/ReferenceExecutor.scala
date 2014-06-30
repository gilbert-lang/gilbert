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

package org.gilbertlang.runtime.execution.reference

import breeze.stats.distributions.Gaussian
import breeze.linalg.{min, max, norm, *}
import org.gilbertlang.runtime._
import org.gilbertlang.runtime.Executables._
import org.gilbertlang.runtime.Operations._
import org.gilbertlang.runtimeMacros.linalg.breeze.operators.{BreezeMatrixOps}
import org.gilbertlang.runtimeMacros.linalg.operators.{DoubleVectorImplicits, DoubleMatrixImplicits}
import scala.io.Source
import org.gilbertlang.runtime.shell.PlanPrinter
import org.gilbertlang.runtimeMacros.linalg.{MatrixFactory, DoubleMatrix, Configuration, DoubleMatrixFactory}
import org.gilbertlang.runtime.execution.stratosphere.GaussianRandom
import org.gilbertlang.runtimeMacros.linalg.breeze.operators.BreezeMatrixRegistries
import util.control.Breaks.{break, breakable}
import org.gilbertlang.runtime.RuntimeTypes.{MatrixType, DoubleType, BooleanType}
import org.gilbertlang.runtime.execution.UtilityFunctions.binarize
import scala.language.postfixOps

class ReferenceExecutor extends Executor with BreezeMatrixOps with BreezeMatrixRegistries with DoubleMatrixImplicits
with DoubleVectorImplicits {

  type Matrix[T] = breeze.linalg.Matrix[T]
  type CellArray = List[Any]

  //TODO fix this
  var iterationState: DoubleMatrix = null
  var convergenceCurrentStateMatrix: DoubleMatrix = null
  var convergencePreviousStateMatrix: DoubleMatrix = null
  var iterationStateCellArray: CellArray = null
  var convergenceCurrentStateCellArray: CellArray = null
  var convergencePreviousStateCellArray: CellArray = null

  implicit var doubleMatrixFacotry = MatrixFactory.doubleMatrixFactory
  implicit var booleanMatrixFactory = MatrixFactory.booleanMatrixFactory

  protected def execute(executable: Executable): Any = {

    executable match {
      case (compound: CompoundExecutable) =>
        compound.executables foreach { execute }
      case VoidExecutable => ()
      case transformation: LoadMatrix =>

        handle[LoadMatrix, (String, Int, Int)](transformation,
          { transformation => {
            (evaluate[String](transformation.path), evaluate[Double](transformation.numRows).toInt,
                evaluate[Double](transformation.numColumns).toInt) }},
          { case (_, (path, numRows, numColumns)) =>
            val itEntries = for(line <- Source.fromFile(path).getLines()) yield {
              val splits = line.split(" ")
              (splits(0).toInt-1, splits(1).toInt-1, splits(2).toDouble)
            }
            val entries = itEntries.toSeq
            val dense = entries.length.toDouble/(numRows* numColumns) > Configuration.DENSITYTHRESHOLD

            val factory = implicitly[DoubleMatrixFactory]
            factory.create(numRows, numColumns, entries, dense)
          })

      case (transformation: FixpointIterationMatrix) =>
        val (initialIterationState, maxIterations) = (handle[FixpointIterationMatrix, (DoubleMatrix,
          Int)](transformation,
          { transformation => (evaluate[DoubleMatrix](transformation.initialState),
            evaluate[Double](transformation.maxIterations).toInt) },
          { (_, result) => result })).asInstanceOf[(DoubleMatrix, Int)]


        iterationState = initialIterationState
        breakable {for (counter <- 1 to maxIterations) {
          if(transformation.convergencePlan != null){
            convergencePreviousStateMatrix = iterationState
          }

          iterationState = handle[FixpointIterationMatrix, DoubleMatrix](transformation,
            { transformation => evaluate[DoubleMatrix](transformation.updatePlan) },
            { (_, nextIterationState) => nextIterationState }).asInstanceOf[DoubleMatrix]

          if(transformation.convergencePlan != null){
            convergenceCurrentStateMatrix = iterationState
            val converged = evaluate[Boolean](transformation.convergencePlan)

            if(converged){
              break
            }
          }
        }}

        iterationState

      case ConvergenceCurrentStatePlaceholder => convergenceCurrentStateMatrix
      case ConvergencePreviousStatePlaceholder => convergencePreviousStateMatrix
      case IterationStatePlaceholder => iterationState

      case fixpoint: FixpointIterationCellArray =>
        val (initialIterationState, maxIterations) = handle[FixpointIterationCellArray,(CellArray, Int)](
        fixpoint,
        {input => (evaluate[CellArray](input.initialState), evaluate[Double](input.maxIterations).toInt)},
        {(_, initialCellArray) => initialCellArray}
        ).asInstanceOf[(CellArray, Int)]

        iterationStateCellArray = initialIterationState

        breakable { for(iteration <- 0 until maxIterations){
          if(fixpoint.convergencePlan != null){
            convergencePreviousStateCellArray = iterationStateCellArray
          }

          iterationStateCellArray = handle[FixpointIterationCellArray, CellArray](
          fixpoint,
          {input => evaluate[CellArray](input.updatePlan)},
          {(_, nextIterationState) => nextIterationState}
          ).asInstanceOf[CellArray]

          if(fixpoint.convergencePlan != null){
            convergenceCurrentStateCellArray = iterationStateCellArray
            val converged = evaluate[Boolean](fixpoint.convergencePlan)

            if(converged){
              break
            }
          }
        }}

        iterationStateCellArray

      case placeholder: ConvergenceCurrentStateCellArrayPlaceholder => convergenceCurrentStateCellArray
      case placeholder: ConvergencePreviousStateCellArrayPlaceholder => convergencePreviousStateCellArray
      case iterationState: IterationStatePlaceholderCellArray => iterationStateCellArray

      case cellArrayExec: CellArrayExecutable =>
        handle[CellArrayExecutable, List[Any]](
        cellArrayExec,
        {input => input.elements map {evaluate[Any] } },
        {(_, entries) => entries}
        )


      case (transformation: CellwiseMatrixTransformation) =>

        handle[CellwiseMatrixTransformation, DoubleMatrix](transformation,
          { transformation => evaluate[DoubleMatrix](transformation.matrix) },
          { (transformation, matrix) => {
              transformation.operation match {
                case Binarize => matrix.mapActiveValues(binarize)
                case Minus => matrix * -1.0
                case Abs => matrix mapActiveValues(math.abs)
              }
            }
          })

      case transformation: CellwiseMatrixMatrixTransformation =>
        transformation.operation match {
          case logicOperation: LogicOperation =>
            handle[CellwiseMatrixMatrixTransformation, (Matrix[Boolean], Matrix[Boolean])](
            transformation,
            { input => (evaluate[Matrix[Boolean]](input.left), evaluate[Matrix[Boolean]](input.right))},
            { case (_, (left, right)) =>
              logicOperation match {
                case And | SCAnd =>
                  left :& right
                case Or | SCOr =>
                  left :| right
              }
            }
            )
          case operation: ComparisonOperation =>
            handle[CellwiseMatrixMatrixTransformation, (DoubleMatrix, DoubleMatrix)](
            transformation,
            {input => (evaluate[DoubleMatrix](input.left), evaluate[DoubleMatrix](input.right))},
            { case (_, (left, right)) =>
              operation match {
                case GreaterThan => left :> right
                case GreaterEqualThan => left :>= right
                case LessThan => left :< right
                case LessEqualThan => left :<= right
                case Equals => left :== right
                case NotEquals => left :!= right
              }
            }
            )
          case operation: ArithmeticOperation =>
            handle[CellwiseMatrixMatrixTransformation, (DoubleMatrix, DoubleMatrix)](
            transformation,
            {input => (evaluate[DoubleMatrix](input.left), evaluate[DoubleMatrix](input.right))},
            { case (_, (left, right)) =>
              operation match {
                case Addition => left + right
                case Subtraction => left - right
                case Multiplication => left :* right
                case Division => left / right
                case Exponentiation => left :^ right
              }
            }
            )
          case operation: MinMax =>
            handle[CellwiseMatrixMatrixTransformation, (DoubleMatrix, DoubleMatrix)](
            transformation,
            {input => (evaluate[DoubleMatrix](input.left), evaluate[DoubleMatrix](input.right))},
            { case (_, (left, right)) =>
              operation match {
                case Maximum => max(left, right)
                case Minimum => min(left,right)
              }
            }
            )
        }

      case (transformation: Transpose) =>

        handle[Transpose, DoubleMatrix](transformation,
          { transformation => evaluate[DoubleMatrix](transformation.matrix) },
          { (transformation, matrix) => matrix.t })

      case (transformation: MatrixMult) =>

        handle[MatrixMult, (DoubleMatrix, DoubleMatrix)](transformation,
          { transformation => {
              (evaluate[DoubleMatrix](transformation.left), evaluate[DoubleMatrix](transformation.right)) }},
          { case (_, (leftMatrix, rightMatrix)) =>
            leftMatrix * rightMatrix
          })

      case (transformation: AggregateMatrixTransformation) =>

        handle[AggregateMatrixTransformation, DoubleMatrix](transformation,
          { transformation => evaluate[DoubleMatrix](transformation.matrix) },
          { (transformation, matrix) => {
              transformation.operation match {
                case Maximum => max(matrix)
                case Minimum => min(matrix)
                case Norm2 =>
                  val sumOfSquares = breeze.linalg.sum(matrix :* matrix)
                  math.sqrt(sumOfSquares)
                case SumAll => breeze.linalg.sum(matrix)
              }
            }
          })

      case executable: ScalarMatrixTransformation =>
        executable.operation match {
          case logicOperation: LogicOperation =>
            handle[ScalarMatrixTransformation, (Boolean, Matrix[Boolean])](
            executable,
            { exec => (evaluate[Boolean](exec.scalar), evaluate[Matrix[Boolean]](exec.matrix))},
            { case (_, (scalar, matrix)) =>
              logicOperation match {
                case And | SCAnd =>
                  matrix :& scalar
                case Or | SCOr =>
                  matrix :| scalar
              }
            })
          case operation: ComparisonOperation =>
            handle[ScalarMatrixTransformation, (Double, DoubleMatrix)](
            executable,
            { exec => (evaluate[Double](exec.scalar), evaluate[DoubleMatrix](exec.matrix)) },
            {
              case (_, (scalar, matrix)) =>
                operation match {
                  case GreaterThan =>
                    matrix :< scalar
                  case GreaterEqualThan =>
                    matrix :<= scalar
                  case LessThan =>
                    matrix :> scalar
                  case LessEqualThan =>
                    matrix :>= scalar
                  case Equals =>
                    matrix :== scalar
                  case NotEquals =>
                    matrix :!= scalar
                }
            })
          case operation: ArithmeticOperation =>
            handle[ScalarMatrixTransformation, (Double, DoubleMatrix)](
            executable,
            { exec => (evaluate[Double](exec.scalar), evaluate[DoubleMatrix](exec.matrix)) },
            {
              case (_, (scalar, matrix)) =>
                operation match {
                  case Addition =>
                    matrix + scalar
                  case Subtraction =>
                    matrix + -scalar
                  case Multiplication =>
                    matrix * scalar
                  case Division =>
                    val factory = implicitly[DoubleMatrixFactory]
                    val dividend = factory.init(matrix.rows, matrix.cols, scalar, dense = true)
                    dividend / matrix
                  case Exponentiation =>
                    val factory = implicitly[DoubleMatrixFactory]
                    val basis = factory.init(matrix.rows, matrix.cols, scalar,dense = true)
                    basis :^ matrix
                }
            })
        }

      case executable: MatrixScalarTransformation =>
        executable.operation match {
          case logicOperation: LogicOperation =>
            handle[MatrixScalarTransformation, (Matrix[Boolean], Boolean)](
            executable,
            {exec => (evaluate[Matrix[Boolean]](exec.matrix), evaluate[Boolean](exec.scalar))},
            { case (_, (matrix, scalar)) =>
              logicOperation match {
                case And | SCAnd =>
                  matrix :& scalar
                case Or | SCOr =>
                  matrix :| scalar
              }
            })
          case operation : ComparisonOperation =>
            handle[MatrixScalarTransformation, (DoubleMatrix,Double)](
            executable,
            { exec => (evaluate[DoubleMatrix](exec.matrix), evaluate[Double](exec.scalar)) },
            {
              case (_, (matrix, scalar)) =>
                operation match {
                  case GreaterThan =>
                    matrix :> scalar
                  case GreaterEqualThan =>
                    matrix :>= scalar
                  case LessThan =>
                    matrix :< scalar
                  case LessEqualThan =>
                    matrix :<= scalar
                  case Equals =>
                    matrix :== scalar
                  case NotEquals =>
                    matrix :!= scalar
                }
            })
          case operation : ArithmeticOperation =>
            handle[MatrixScalarTransformation, (DoubleMatrix,Double)](
            executable,
            { exec => (evaluate[DoubleMatrix](exec.matrix), evaluate[Double](exec.scalar)) },
            {
              case (_, (matrix, scalar)) =>
                operation match {
                  case Addition =>
                    matrix + scalar
                  case Subtraction =>
                    matrix - scalar
                  case Multiplication =>
                    matrix * scalar
                  case Division =>
                    matrix / scalar
                  case Exponentiation =>
                    matrix :^ scalar
                }
            })
        }

      case (transformation: VectorwiseMatrixTransformation) =>

        handle[VectorwiseMatrixTransformation, DoubleMatrix](transformation,
          { transformation => evaluate[DoubleMatrix](transformation.matrix) },
          { (transformation, matrix) => {
              transformation.operation match {
                case NormalizeL1 =>
                  val l1norm = norm(matrix(*, ::),1.0)
                  val result = matrix.copy
                  for(col <- 0 until matrix.cols){
                    val vector = result(::, col)
                    result(::, col) :/= l1norm
                  }
                  result
                case Maximum =>
                  max(matrix(*, ::))
                case Minimum =>
                  min(matrix(*, ::))
                case Norm2 =>
                  val squaredEntries = matrix :^ 2.0
                  val sumSquaredEntries = breeze.linalg.sum(squaredEntries(*, ::))
                  val result = sumSquaredEntries map { value => math.sqrt(value)}
                  result
              }
            }
          })

      case (transformation: ones) =>
        handle[ones, (Int, Int)](transformation,
          { transformation => (evaluate[Double](transformation.numRows).toInt,
              evaluate[Double](transformation.numColumns).toInt) },
          { case (_, (numRows, numColumns)) =>
            val factory = implicitly[DoubleMatrixFactory]
            factory.init(numRows, numColumns, 1.0, dense = true)
          })

      case (transformation: eye) =>
        handle[eye, (Int, Int)](
          transformation,
          { trans => (evaluate[Double](trans.numRows).toInt, evaluate[Double](trans.numCols).toInt)},
          { case (_, (rows, cols)) =>
            val factory = implicitly[DoubleMatrixFactory]
            factory.eye(rows, cols, math.min(rows, cols).toDouble/(rows*cols) > Configuration.DENSITYTHRESHOLD)
          }
        )

      case (transformation: zeros) =>
        handle[zeros, (Int, Int)](
            transformation,
            {transformation => (evaluate[Double](transformation.numRows).toInt,
                evaluate[Double](transformation.numCols).toInt)},
            { case (_, (rows, cols)) =>
              val factory = implicitly[DoubleMatrixFactory]
              factory.create(rows, cols, dense = false)
            })

      case (transformation: randn) =>

        handle[randn, (Int, Int, Double, Double)](transformation,
          { transformation =>
              (evaluate[Double](transformation.numRows).toInt, evaluate[Double](transformation.numColumns).toInt,
                  evaluate[Double](transformation.mean), evaluate[Double](transformation.std)) },
          { case (_, (numRows, numColumns, mean, std)) =>
            val random = new GaussianRandom(mean, std)
            val rand = new Gaussian(mean, std)
            val factory = implicitly[DoubleMatrixFactory]
            factory.rand(numRows, numColumns, rand)
          })

      case transformation: spones =>
        handle[spones, DoubleMatrix](transformation,
            { transformation => evaluate[DoubleMatrix](transformation.matrix) },
            { (_, matrix) => matrix mapActiveValues { binarize } })

      //TODO remove this
      case transformation: sum =>
        handle[sum, (DoubleMatrix, Int)](transformation,
            { transformation => (evaluate[DoubleMatrix](transformation.matrix),
                evaluate[Double](transformation.dimension).toInt) },
            { case (_, (matrix, dimension)) =>
              if(dimension == 1){
                breeze.linalg.sum(matrix(::, *))
              }else{
                breeze.linalg.sum(matrix(*, ::)).asMatrix
              }
            })

      case transformation: sumRow =>
        handle[sumRow, DoubleMatrix](transformation,
            { transformation => evaluate[DoubleMatrix](transformation.matrix) },
            { (_, matrix) => {
              breeze.linalg.sum(matrix(*, ::)).asMatrix
            }})

      case transformation: sumCol =>
        handle[sumCol, DoubleMatrix](transformation,
            { transformation => evaluate[DoubleMatrix](transformation.matrix) },
            { (_, matrix) => {
              breeze.linalg.sum(matrix(::, *))
            }})

      //TODO substitute with specialized operators
      case transformation: diag =>
        handle[diag, DoubleMatrix](transformation,
            {transformation => evaluate[DoubleMatrix](transformation.matrix)},
            { (_, matrix) => {
              (matrix.rows, matrix.cols) match {
                case (1, x) =>
                  val entries = (matrix.activeIterator map { case ((row, col), value) => (col, col,
                    value)}).toArray[(Int, Int, Double)]
                  val factory = implicitly[DoubleMatrixFactory]
                  factory.create(x,x, entries, entries.length.toDouble/(x*x) > Configuration.DENSITYTHRESHOLD)
                case (x, 1) =>
                  val entries = (matrix.activeIterator map { case ((row, col), value) => (row, row,
                    value)}).toArray[(Int, Int, Double)]
                  val factory = implicitly[DoubleMatrixFactory]
                  factory.create(x,x, entries, entries.length.toDouble/(x*x) > Configuration.DENSITYTHRESHOLD)
                case (x:Int,y:Int) =>
                  val minimum = math.min(x,y)
                  val factory = implicitly[DoubleMatrixFactory]
                  val itEntries = for(idx <- 0 until minimum) yield (0,idx,matrix(idx,idx))
                  val entries = itEntries.toSeq
                  factory.create(minimum, 1, entries, dense = true)
              }
            }})

      case (transformation: WriteMatrix) =>
        transformation.matrix.getType match {
          case MatrixType(DoubleType, _, _) =>
            handle[WriteMatrix, DoubleMatrix](transformation,
            { transformation => evaluate[DoubleMatrix](transformation.matrix) },
            { (_, matrix) => println(matrix) })
          case MatrixType(BooleanType, _,_) =>
            handle[WriteMatrix, Matrix[Boolean]](transformation,
            { transformation => evaluate[Matrix[Boolean]](transformation.matrix) },
            { (_, matrix) => println(matrix) })
        }


      case transformation: WriteString =>

        handle[WriteString, String](transformation,
            { transformation => evaluate[String](transformation.string) },
            { (_, string) => println(string) })

      case transformation: WriteFunction =>
        handle[WriteFunction, Unit](transformation,
            { _ => },
            { (transformation, _) => PlanPrinter.print(transformation.function) })


      case (transformation: scalar) =>

        handle[scalar, Unit](transformation,
          { _ => },
          { (transformation, _) => transformation.value })

      case literal: boolean =>
        handle[boolean, Unit](literal,
        {_ => },
        {(literal, _) => literal.value})

      case transformation: string =>
        handle[string, Unit](transformation,
          { _ => },
          { (transformation, _) => transformation.value })

      case (transformation: WriteScalar) =>
        transformation.scalar.getType match {
          case BooleanType =>
            handle[WriteScalar, Boolean](transformation,
            { transformation => evaluate[Boolean](transformation.scalar) },
            { (_, scalar) => println(scalar)} )
          case DoubleType =>
            handle[WriteScalar, Double](transformation,
            { transformation => evaluate[Double](transformation.scalar) },
            { (_, scalar) => println(scalar) })
          case tpe =>
            throw new LocalExecutionError(s"Cannot print scalar of type $tpe.")
        }

      case writeCellArray: WriteCellArray =>
        handle[WriteCellArray, CellArray](
        writeCellArray,
        {input => evaluate[CellArray](input.cellArray)},
        {(_, cellArray) => for(entry <- cellArray) println(entry)}
        )


      case transformation: UnaryScalarTransformation =>
        handle[UnaryScalarTransformation, Double](transformation,
          { transformation => evaluate[Double](transformation.scalar) },
          { (transformation, value) =>
            transformation.operation match {
              case Minus => -value
              case Binarize => binarize(value)
              case Abs => math.abs(value)
            }
          })

      case transformation: ScalarScalarTransformation =>
        handle[ScalarScalarTransformation, (Double, Double)](transformation,
          { transformation => (evaluate[Double](transformation.left), evaluate[Double](transformation.right)) },
          {
            case (exec, (left, right)) =>
              exec.operation match {
              case Addition => left + right
              case Subtraction => left - right
              case Division => left / right
              case Multiplication => left * right
              case GreaterThan => left > right
              case GreaterEqualThan => left >= right
              case LessThan => left < right
              case LessEqualThan => left <= right
              case Equals => left == right
              case NotEquals => left != right
              case SCAnd => left && right
              case SCOr => left || right
              case And => left & right
              case Or => left | right
              case Maximum => math.max(left, right)
              case Minimum => math.min(left, right)
              case Exponentiation => math.pow(left, right)
              }
          })

      case transformation: Parameter =>
        throw new ExecutionRuntimeError("Parameters cannot be executed")

      case transformation: function =>
        throw new ExecutionRuntimeError("Functions cannot be executed")

      case reference: CellArrayReferenceString =>
        handle[CellArrayReferenceString, CellArray](
        reference,
        {input => evaluate[CellArray](input.parent)},
        { (ref, cellArray) =>
          cellArray(ref.reference).asInstanceOf[String]
        }
        )

      case reference: CellArrayReferenceScalar =>
        handle[CellArrayReferenceScalar, CellArray](
        reference,
        {input => evaluate[CellArray](input.parent)},
        {(ref, cellArray) =>
          ref.getType match {
            case DoubleType => cellArray(ref.reference).asInstanceOf[Double]
            case BooleanType => cellArray(ref.reference).asInstanceOf[Boolean]
            case tpe => throw new LocalExecutionError(s"Cannot reference scalar value of type $tpe.")
          }
        }
        )

      case reference: CellArrayReferenceMatrix =>
        handle[CellArrayReferenceMatrix, CellArray](
        reference,
        {input => evaluate[CellArray](input.parent)},
        {(ref, cellArray) =>
          ref.getType match {
            case MatrixType(DoubleType, _, _) => cellArray(ref.reference).asInstanceOf[DoubleMatrix]
            case MatrixType(BooleanType, _, _) => cellArray(ref.reference).asInstanceOf[Matrix[Boolean]]
            case tpe => throw new LocalExecutionError(s"Cannot reference matrix of type $tpe.")
          }
        }
        )

      case reference: CellArrayReferenceCellArray =>
        handle[CellArrayReferenceCellArray, CellArray](
        reference,
        {input => evaluate[CellArray](input.parent)},
        {(ref, cellArray) => cellArray(ref.reference).asInstanceOf[List[Any]]}
        )

      case typeConversion: TypeConversionMatrix =>
        (typeConversion.sourceType, typeConversion.targetType) match {
          case (MatrixType(BooleanType, _, _), MatrixType(DoubleType, _, _)) =>
            handle[TypeConversionMatrix, Matrix[Boolean]](
            typeConversion,
            {input => evaluate[Matrix[Boolean]](input.matrix)},
            {(_, matrix) =>
              matrix mapValues { value => if(value) 1.0 else 0.0}
            }
            )
          case (srcType, targetType) => throw new LocalExecutionError(s"Cannot convert matrix value of type $srcType " +
            s"to type $targetType.")
        }

      case typeConversion: TypeConversionScalar =>
        (typeConversion.sourceType, typeConversion.targetType) match {
          case (BooleanType, DoubleType) =>
            handle[TypeConversionScalar, Boolean](
            typeConversion,
            {input => evaluate[Boolean](input.scalar)},
            {(_, scalar) => if(scalar) 1.0 else 0.0}
            )
          case (srcType, targetType) => throw new LocalExecutionError(s"Cannot convert scalar value of type $srcType " +
            s"to type $targetType.")
        }

      case linearSpace: linspace =>
        handle[linspace, (Double, Double, Int)](
        linearSpace,
        {input => (evaluate[Double](input.start), evaluate[Double](input.end), evaluate[Double](input.numPoints)
          .toInt)},
        {case (_, (start, end, numPoints)) =>
          val factory = implicitly[DoubleMatrixFactory]
          val spacing = (end-start)/(numPoints-1)
          val entries = for(numPoint <- 0 until numPoints) yield(0, numPoint, start + numPoint*spacing)
          factory.create(1,numPoints, entries, true)
        }
        )

      case minWithIdx: minWithIndex =>
        handle[minWithIndex, (DoubleMatrix, Int)](
        minWithIdx,
        {input => (evaluate[DoubleMatrix](input.matrix), evaluate[Double](input.dimension).toInt)},
        {case (_, (matrix, dimension)) =>
          val factory = implicitly[DoubleMatrixFactory]
          val (minimum, minIdx) = dimension match {
            case 1 =>
              val minValues = for(column <- 0 until matrix.cols) yield {
                matrix(::, column).iterator.minBy{case (row, value) => value}
              }
              val(minIdx, minimum) = minValues.unzip

              val (minEntries, minIdxEntries) = ((minimum zipWithIndex) map { case (value, idx) => (0, idx,value)},
                (minIdx zipWithIndex) map { case (value, idx) => (0, idx, (value+1).toDouble)})
              (factory.create(1, matrix.cols, minEntries, true), factory.create(1, matrix.cols, minIdxEntries, true))
            case 2 =>
              val minValues = for(row <- 0 until matrix.rows) yield {
                matrix(row, ::).iterator.minBy{case (_, value) => value }
              }

              val(minIdx, minimum) = minValues.unzip
              val (minEntries, minIdxEntries) = ((minimum zipWithIndex) map { case (value, idx) => (idx, 0, value)},
                (minIdx zipWithIndex) map { case ((_, value), idx) => (idx, 0, (value+1).toDouble)})
              (factory.create(matrix.rows, 1, minEntries, true), factory.create(matrix.rows, 1, minIdxEntries,true))
            case dim => throw new LocalExecutionError(s"Cannot execute minWithIndex for dimension $dim.")
          }
          List(minimum, minIdx)
        }
        )

      case pairDistance: pdist2 =>
        handle[pdist2, (DoubleMatrix, DoubleMatrix)](
        pairDistance,
        {input => (evaluate[DoubleMatrix](input.matrixA), evaluate[DoubleMatrix](input.matrixB))},
        {case (_, (matrixA, matrixB)) =>
          val factory = implicitly[DoubleMatrixFactory]
          val entries = for(rowA <- 0 until matrixA.rows; rowB <- 0 until matrixB.rows) yield {
            val value = math.sqrt(breeze.linalg.sum((matrixA(rowA, ::) - matrixB(rowB, ::)):^2.0))
            (rowA, rowB, value)
          }

          val temp = entries.toArray[(Int,Int,Double)]

          factory.create(matrixA.rows, matrixB.rows, entries, true)
        }
        )


      case repeatMatrix: repmat =>
        handle[repmat, (DoubleMatrix, Int, Int)](
        repeatMatrix,
        {input => (evaluate[DoubleMatrix](input.matrix), evaluate[Double](input.numRows).toInt,
          evaluate[Double](input.numCols).toInt)},
        { case (_, (matrix, rowsMult, colsMult)) =>
          val factory = implicitly[DoubleMatrixFactory]

          val entries = matrix.activeIterator flatMap {
            case ((row, col),value) =>
              for(rowMult <- 0 until rowsMult; colMult <- 0 until colsMult) yield {
                (row + rowMult*matrix.rows, col + colMult*matrix.cols, value)
              }
          }

          val newRows = matrix.rows*rowsMult
          val newCols = matrix.cols*colsMult
          val newSize = newRows* newCols
          val seqEntries = entries.toSeq
          factory.create(newRows, newCols, seqEntries, seqEntries.length.toDouble/(newSize) > Configuration.
            DENSITYTHRESHOLD)
        }
        )

    }

  }
}

