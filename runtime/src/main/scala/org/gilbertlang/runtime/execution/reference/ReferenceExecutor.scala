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

import org.gilbertlang.runtime._
import org.gilbertlang.runtime.Executables._
import org.gilbertlang.runtime.Operations._
import scala.io.Source
import org.gilbertlang.runtime.execution.CellwiseFunctions
import org.gilbertlang.runtime.shell.PlanPrinter
import org.gilbertlang.runtimeMacros.linalg.{numerics, Configuration, MatrixFactory}
import breeze.linalg.{DenseMatrix, norm, *}
import org.gilbertlang.runtime.execution.stratosphere.GaussianRandom
import org.gilbertlang.runtimeMacros.linalg.operators.{BreezeMatrixImplicits, BreezeMatrixRegistries, BreezeMatrixOps}

class ReferenceExecutor extends Executor with BreezeMatrixOps with BreezeMatrixRegistries with BreezeMatrixImplicits {

  type Matrix[T] = breeze.linalg.Matrix[T]

  //TODO fix this
  var iterationState: Matrix[Double] = null

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
          { case (transformation, (path, numRows, numColumns)) =>
            val itEntries = for(line <- Source.fromFile(path).getLines()) yield {
              val splits = line.split(" ")
              (splits(0).toInt, splits(1).toInt, splits(2).toDouble)
            }
            val entries = itEntries.toSeq
            val dense = entries.length.toDouble/(numRows* numColumns) > Configuration.DENSITYTHRESHOLD

            val factory = implicitly[MatrixFactory[Double]]
            factory.create(numRows, numColumns, entries, dense)
          })

      case (transformation: FixpointIteration) =>

        iterationState = handle[FixpointIteration, Matrix[Double]](transformation,
          { transformation => evaluate[Matrix[Double]](transformation.initialState) },
          { (_, initialVector) => initialVector }).asInstanceOf[Matrix[Double]]

        for (counter <- 1 to 10) {
          iterationState = handle[FixpointIteration, Matrix[Double]](transformation,
            { transformation => evaluate[Matrix[Double]](transformation.updatePlan) },
            { (_, vector) => vector }).asInstanceOf[Matrix[Double]]
        }

        iterationState

      case IterationStatePlaceholder => iterationState

      case (transformation: CellwiseMatrixTransformation) =>

        handle[CellwiseMatrixTransformation, Matrix[Double]](transformation,
          { transformation => evaluate[Matrix[Double]](transformation.matrix) },
          { (transformation, matrix) => {
              transformation.operation match {
                case Binarize => matrix.mapActiveValues(x => CellwiseFunctions.binarize(x))
                case Minus => matrix * -1.0
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
                case And =>
                  left :& right
                case Or =>
                  left :| right
              }
            }
            )
          case operation: ComparisonOperation =>
            handle[CellwiseMatrixMatrixTransformation, (Matrix[Double], Matrix[Double])](
            transformation,
            {input => (evaluate[Matrix[Double]](input.left), evaluate[Matrix[Double]](input.right))},
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
          case operation =>
            handle[CellwiseMatrixMatrixTransformation, (Matrix[Double], Matrix[Double])](
            transformation,
            {input => (evaluate[Matrix[Double]](input.left), evaluate[Matrix[Double]](input.right))},
            { case (_, (left, right)) =>
              operation match {
                case Addition => left + right
                case Subtraction => left - right
                case Multiplication => left :* right
                case Division => left / right
                case Maximum => numerics.max(left, right)
                case Minimum => numerics.min(left,right)
              }
            }
            )
        }

      case (transformation: Transpose) =>

        handle[Transpose, Matrix[Double]](transformation,
          { transformation => evaluate[Matrix[Double]](transformation.matrix) },
          { (transformation, matrix) => matrix.t })

      case (transformation: MatrixMult) =>

        handle[MatrixMult, (Matrix[Double], Matrix[Double])](transformation,
          { transformation => {
              (evaluate[Matrix[Double]](transformation.left), evaluate[Matrix[Double]](transformation.right)) }},
          { case (_, (leftMatrix, rightMatrix)) => leftMatrix * rightMatrix })

      case (transformation: AggregateMatrixTransformation) =>

        handle[AggregateMatrixTransformation, Matrix[Double]](transformation,
          { transformation => evaluate[Matrix[Double]](transformation.matrix) },
          { (transformation, matrix) => {
              transformation.operation match {
                case Maximum => matrix.max
                case Minimum => matrix.min
                case Norm2 =>
                  val sumOfSquares = breeze.linalg.sum(matrix :* matrix)
                  math.sqrt(sumOfSquares)
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
                case And =>
                  matrix :& scalar
                case Or =>
                  matrix :| scalar
              }
            })
          case operation =>
            handle[ScalarMatrixTransformation, (Double, Matrix[Double])](
            executable,
            { exec => (evaluate[Double](exec.scalar), evaluate[Matrix[Double]](exec.matrix)) },
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
                    val factory = implicitly[MatrixFactory[Double]]
                    val dividend = factory.init(matrix.rows, matrix.cols, scalar, dense = true)
                    dividend / matrix
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
        }

      case executable: MatrixScalarTransformation =>
        executable.operation match {
          case logicOperation: LogicOperation =>
            handle[MatrixScalarTransformation, (Matrix[Boolean], Boolean)](
            executable,
            {exec => (evaluate[Matrix[Boolean]](exec.matrix), evaluate[Boolean](exec.scalar))},
            { case (_, (matrix, scalar)) =>
              logicOperation match {
                case And =>
                  matrix :& scalar
                case Or =>
                  matrix :| scalar
              }
            })
          case operation =>
            handle[MatrixScalarTransformation, (Matrix[Double],Double)](
            executable,
            { exec => (evaluate[Matrix[Double]](exec.matrix), evaluate[Double](exec.scalar)) },
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
        }

      case (transformation: VectorwiseMatrixTransformation) =>

        handle[VectorwiseMatrixTransformation, Matrix[Double]](transformation,
          { transformation => evaluate[Matrix[Double]](transformation.matrix) },
          { (transformation, matrix) => {
              transformation.operation match {
                case NormalizeL1 =>
                  val l1norm = norm(matrix( * , ::),1)
                  val result = matrix.copy
                  for(col <- 0 until matrix.cols){
                    result(::, col) :/= l1norm
                  }

                  result
                case Maximum =>
                  numerics.max(matrix(*, ::))
                case Minimum =>
                  numerics.min(matrix(*, ::))
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
            val factory = implicitly[MatrixFactory[Double]]
            factory.init(numRows, numColumns, 1.0, dense = true)
          })

      case (transformation: eye) =>
        handle[eye, (Int, Int)](
          transformation,
          { trans => (evaluate[Double](trans.numRows).toInt, evaluate[Double](trans.numCols).toInt)},
          { case (_, (rows, cols)) =>
            val factory = implicitly[MatrixFactory[Double]]
            factory.eye(rows, cols, math.min(rows, cols).toDouble/(rows*cols) > Configuration.DENSITYTHRESHOLD)
          }
        )

      case (transformation: zeros) =>
        handle[zeros, (Int, Int)](
            transformation,
            {transformation => (evaluate[Double](transformation.numRows).toInt,
                evaluate[Double](transformation.numCols).toInt)},
            { case (_, (rows, cols)) =>
              val factory = implicitly[MatrixFactory[Double]]
              factory.create(rows, cols, dense = false)
            })

      case (transformation: randn) =>

        handle[randn, (Int, Int, Double, Double)](transformation,
          { transformation =>
              (evaluate[Double](transformation.numRows).toInt, evaluate[Double](transformation.numColumns).toInt,
                  evaluate[Double](transformation.mean), evaluate[Double](transformation.std)) },
          { case (_, (numRows, numColumns, mean, std)) =>
            val random = new GaussianRandom(mean, std)
            DenseMatrix.rand(numRows, numColumns, random)
          })

      case transformation: spones =>
        handle[spones, Matrix[Double]](transformation,
            { transformation => evaluate[Matrix[Double]](transformation.matrix) },
            { (_, matrix) => matrix mapActiveValues { value => CellwiseFunctions.binarize(value) } })

      //TODO remove this
      case transformation: sum =>
        handle[sum, (Matrix[Double], Int)](transformation,
            { transformation => (evaluate[Matrix[Double]](transformation.matrix),
                evaluate[Double](transformation.dimension).toInt) },
            { case (_, (matrix, dimension)) =>
              if(dimension == 1){
                breeze.linalg.sum(matrix(::, *))
              }else{
                breeze.linalg.sum(matrix(*, ::)).asMatrix
              }
            })

      case transformation: sumRow =>
        handle[sumRow, Matrix[Double]](transformation,
            { transformation => evaluate[Matrix[Double]](transformation.matrix) },
            { (_, matrix) => {
              breeze.linalg.sum(matrix(*, ::)).asMatrix
            }})

      case transformation: sumCol =>
        handle[sumCol, Matrix[Double]](transformation,
            { transformation => evaluate[Matrix[Double]](transformation.matrix) },
            { (_, matrix) => {
              breeze.linalg.sum(matrix(::, *))
            }})

      //TODO substitute with specialized operators
      case transformation: diag =>
        handle[diag, Matrix[Double]](transformation,
            {transformation => evaluate[Matrix[Double]](transformation.matrix)},
            { (_, matrix) => {
              (matrix.rows, matrix.cols) match {
                case (1, x) =>
                  val entries = (matrix.activeIterator map { case ((row, col), value) => (col, col,
                    value)}).toArray[(Int, Int, Double)]
                  val factory = implicitly[MatrixFactory[Double]]
                  factory.create(x,x, entries, entries.length.toDouble/(x*x) > Configuration.DENSITYTHRESHOLD)
                case (x, 1) =>
                  val entries = (matrix.activeIterator map { case ((row, col), value) => (row, row,
                    value)}).toArray[(Int, Int, Double)]
                  val factory = implicitly[MatrixFactory[Double]]
                  factory.create(x,x, entries, entries.length.toDouble/(x*x) > Configuration.DENSITYTHRESHOLD)
                case (x:Int,y:Int) =>
                  val minimum = math.min(x,y)
                  val factory = implicitly[MatrixFactory[Double]]
                  val itEntries = for(idx <- 0 until minimum) yield (0,idx,matrix(idx,idx))
                  val entries = itEntries.toSeq
                  factory.create(minimum, 1, entries, dense = true)
              }
            }})

      case (transformation: WriteMatrix) =>

        handle[WriteMatrix, Matrix[Double]](transformation,
          { transformation => evaluate[Matrix[Double]](transformation.matrix) },
          { (_, matrix) => println(matrix) })

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

      case transformation: string =>
        handle[string, Unit](transformation,
          { _ => },
          { (transformation, _) => transformation.value })

      case (transformation: WriteScalar) =>

        handle[WriteScalar, Double](transformation,
          { transformation => evaluate[Double](transformation.scalar) },
          { (_, scalar) => println(scalar) })

      case transformation: UnaryScalarTransformation =>
        handle[UnaryScalarTransformation, Double](transformation,
          { transformation => evaluate[Double](transformation.scalar) },
          { (transformation, value) =>
            transformation.operation match {
              case Minus => -value
              case Binarize => CellwiseFunctions.binarize(value)
            }
          })

      case transformation: ScalarScalarTransformation =>
        handle[ScalarScalarTransformation, (Double, Double)](transformation,
          { transformation => (evaluate[Double](transformation.left), evaluate[Double](transformation.right)) },
          {
            case (transformation, (left, right)) =>
              val result: Double = transformation.operation match {
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
              case And => left && right
              case Or => left || right
              case Maximum => math.max(left, right)
              case Minimum => math.min(left, right)
              }
              result
          })

      case transformation: Parameter =>
        throw new ExecutionRuntimeError("Parameters cannot be executed")

      case transformation: function =>
        throw new ExecutionRuntimeError("Functions cannot be executed")
    }

  }
}

