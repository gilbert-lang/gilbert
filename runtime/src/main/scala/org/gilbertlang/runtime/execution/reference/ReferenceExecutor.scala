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
import org.apache.mahout.math.{DenseMatrix, SparseRowMatrix}
import org.gilbertlang.runtime.execution.VectorFunctions
import org.apache.mahout.math.random.Normal
import org.gilbertlang.runtime.Executables._
import org.gilbertlang.runtime.Operations._
import scala.io.Source
import org.apache.mahout.math.SparseMatrix
import org.gilbertlang.runtime.execution.CellwiseFunctions
import org.apache.mahout.math.DiagonalMatrix
import org.gilbertlang.runtime.shell.PlanPrinter

class ReferenceExecutor extends Executor {

  type MahoutMatrix = org.apache.mahout.math.Matrix

  //TODO fix this
  var iterationState: MahoutMatrix = null

  protected def execute(executable: Executable): Any = {

    executable match {
      case (compound: CompoundExecutable) => {
        compound.executables foreach { execute }
      }
      case VoidExecutable => ()
      case transformation: LoadMatrix => {

        handle[LoadMatrix, (String, Int, Int)](transformation,
          { transformation => {
            (evaluate[String](transformation.path), evaluate[Double](transformation.numRows).toInt,
                evaluate[Double](transformation.numColumns).toInt) }},
          { case (transformation, (path, numRows, numColumns)) => {

              val matrix = new SparseRowMatrix(numRows, numColumns)

              for (line <- Source.fromFile(path).getLines()) {
                val fields = line.split(" ")
                matrix.setQuick(fields(0).toInt - 1, fields(1).toInt - 1, fields(2).toDouble)
              }

              matrix
            }
          })
      }

      case (transformation: FixpointIteration) => {

        iterationState = handle[FixpointIteration, MahoutMatrix](transformation,
          { transformation => evaluate[MahoutMatrix](transformation.initialState) },
          { (_, initialVector) => initialVector }).asInstanceOf[MahoutMatrix]

        for (counter <- 1 to 10) {
          iterationState = handle[FixpointIteration, MahoutMatrix](transformation,
            { transformation => evaluate[MahoutMatrix](transformation.updatePlan) },
            { (_, vector) => vector }).asInstanceOf[MahoutMatrix]
        }

        iterationState
      }

      case IterationStatePlaceholder => iterationState

      case (transformation: CellwiseMatrixTransformation) => {

        handle[CellwiseMatrixTransformation, MahoutMatrix](transformation,
          { transformation => evaluate[MahoutMatrix](transformation.matrix) },
          { (transformation, matrix) => {
              transformation.operation match {
                case Binarize => matrix.assign(VectorFunctions.binarize)
                case Minus => matrix.times(-1)
              }
            }
          })
      }

      case transformation: CellwiseMatrixMatrixTransformation => {
        handle[CellwiseMatrixMatrixTransformation, (MahoutMatrix, MahoutMatrix)](transformation,
          { transformation =>
              (evaluate[MahoutMatrix](transformation.left), evaluate[MahoutMatrix](transformation.right)) },
          { case (transformation, (left, right)) => {
              transformation.operation match {
                case Addition => left.plus(right)
                case Subtraction => left.minus(right)
                case Multiplication =>
                  val result = CloneHelper.clone(left)
                  result.assign(right, CellwiseFunctions.times)
                case Division => 
                  val result = CloneHelper.clone(left)
                  result.assign(right, CellwiseFunctions.divide)
                case GreaterThan =>
                  val result = CloneHelper.clone(left)
                  result.assign(right, CellwiseFunctions.greaterThan)
                case GreaterEqualThan =>
                  val result = CloneHelper.clone(left)
                  result.assign(right, CellwiseFunctions.greaterEqualThan)
                case LessThan =>
                  val result = CloneHelper.clone(left)
                  result.assign(right, CellwiseFunctions.lessThan)
                case LessEqualThan =>
                  val result = CloneHelper.clone(left)
                  result.assign(right, CellwiseFunctions.lessEqualThan)
                case Equals =>
                  val result = CloneHelper.clone(left)
                  result.assign(right, CellwiseFunctions.equals)
                case And =>
                  val result = CloneHelper.clone(left)
                  result.assign(right, CellwiseFunctions.logicalAnd)
                case Or =>
                  val result = CloneHelper.clone(left)
                  result.assign(right, CellwiseFunctions.logicalOr)
                //TODO do we need this?
                case Maximum => 
                  val result = CloneHelper.clone(left)
                  result.assign(right, CellwiseFunctions.max)
                //TODO do we need this?
                case Minimum => 
                  val result = CloneHelper.clone(left)
                  result.assign(right, CellwiseFunctions.min)
              }
            }
          })
      }

      case (transformation: Transpose) => {

        handle[Transpose, MahoutMatrix](transformation,
          { transformation => evaluate[MahoutMatrix](transformation.matrix) },
          { (transformation, matrix) => matrix.transpose() })
      }

      case (transformation: MatrixMult) => {

        handle[MatrixMult, (MahoutMatrix, MahoutMatrix)](transformation,
          { transformation => {
              (evaluate[MahoutMatrix](transformation.left), evaluate[MahoutMatrix](transformation.right)) }},
          { case (_, (leftMatrix, rightMatrix)) => leftMatrix.times(rightMatrix) })
      }

      case (transformation: AggregateMatrixTransformation) => {

        handle[AggregateMatrixTransformation, MahoutMatrix](transformation,
          { transformation => evaluate[MahoutMatrix](transformation.matrix) },
          { (transformation, matrix) => {
              transformation.operation match {
                case Maximum => matrix.aggregate(VectorFunctions.max, VectorFunctions.identity)
                case Minimum => matrix.aggregate(VectorFunctions.min, VectorFunctions.identity)
                case Norm2 => {
                  val sumOfSquaredEntries = matrix.aggregateRows(VectorFunctions.lengthSquared).zSum()
                  math.sqrt(sumOfSquaredEntries)
                }
              }
            }
          })
      }

      case (transformation: ScalarMatrixTransformation) => {

        handle[ScalarMatrixTransformation, (MahoutMatrix, Double)](transformation,
          { transformation => {
            (evaluate[MahoutMatrix](transformation.matrix), (evaluate[Double](transformation.scalar)))
          }},
          { case (transformation, (matrix, scalar)) => {
              transformation.operation match {
                case Division => {
                  val dividend = new SparseMatrix(matrix.rowSize(), matrix.columnSize()).assign(scalar)
                  dividend.assign(matrix, CellwiseFunctions.divide)
                }
                case Multiplication => matrix.times(scalar)
                case Addition => matrix.plus(scalar)
                case Subtraction => {
                  val minuend = new SparseMatrix(matrix.rowSize(), matrix.columnSize()).assign(scalar)
                  minuend.minus(matrix)
                }
                case GreaterThan => 
                  val result = CloneHelper.clone(matrix)
                  result.assign({(_:Double) < scalar})
                case GreaterEqualThan =>
                  val result = CloneHelper.clone(matrix)
                  result.assign({(_:Double) <= scalar})
                case LessThan =>
                  val result = CloneHelper.clone(matrix)
                  result.assign({(_:Double) > scalar})
                case LessEqualThan =>
                  val result = CloneHelper.clone(matrix)
                  result.assign({(_:Double) >= scalar})
                case Equals =>
                  val result = CloneHelper.clone(matrix)
                  result.assign({(_:Double) == scalar})
                case And =>
                  val result = CloneHelper.clone(matrix)
                  result.assign({(_:Double) && scalar})
                case Or =>
                  val result = CloneHelper.clone(matrix)
                  result.assign({(_:Double) || scalar})
              }
            }
          })
      }

      case transformation: MatrixScalarTransformation => {
        handle[MatrixScalarTransformation, (MahoutMatrix, Double)](transformation,
          { transformation => {
              (evaluate[MahoutMatrix](transformation.matrix),
                evaluate[Double](transformation.scalar))
          }},
          { case (transformation, (matrix, scalar)) => {
              transformation.operation match {
                case Addition => matrix.plus(scalar)
                case Subtraction => matrix.plus(-scalar)
                case Multiplication => matrix.times(scalar)
                case Division => matrix.divide(scalar)
                case GreaterThan => 
                  val result = CloneHelper.clone(matrix)
                  result.assign({(_:Double) > scalar})
                case GreaterEqualThan =>
                  val result = CloneHelper.clone(matrix)
                  result.assign({(_:Double) >= scalar})
                case LessThan =>
                  val result = CloneHelper.clone(matrix)
                  result.assign({(_:Double) < scalar})
                case LessEqualThan =>
                  val result = CloneHelper.clone(matrix)
                  result.assign({(_:Double) <= scalar})
                case Equals =>
                  val result = CloneHelper.clone(matrix)
                  result.assign({(_:Double) == scalar})
                case And =>
                  val result = CloneHelper.clone(matrix)
                  result.assign({(_:Double) && scalar})
                case Or =>
                  val result = CloneHelper.clone(matrix)
                  result.assign({(_:Double) || scalar})
              }
            }
          })
      }

      case (transformation: VectorwiseMatrixTransformation) => {

        handle[VectorwiseMatrixTransformation, MahoutMatrix](transformation,
          { transformation => evaluate[MahoutMatrix](transformation.matrix) },
          { (transformation, matrix) => {
              transformation.operation match {
                case NormalizeL1 => {
                  for (index <- 0 until matrix.numRows()) {
                    matrix.viewRow(index).normalize(1)
                  }
                  matrix
                }
                case Maximum => {
                  for (index <- 0 until matrix.numRows()) {
                    matrix.viewRow(index).maxValue()
                  }
                  matrix
                }
                case Minimum => {
                  for (index <- 0 until matrix.numRows()) {
                    matrix.viewRow(index).minValue()
                  }
                  matrix
                }
                case Norm2 => {
                  for (index <- 0 until matrix.numRows()) {
                    matrix.viewRow(index).norm(2)
                  }
                  matrix
                }
              }
            }
          })
      }

      case (transformation: ones) => {

        handle[ones, (Int, Int)](transformation,
          { transformation => (evaluate[Double](transformation.numRows).toInt, 
              evaluate[Double](transformation.numColumns).toInt) },
          { case (transformation, (numRows, numColumns)) => { new DenseMatrix(numRows, numColumns).assign(1) }})
      }
      
      case (transformation: eye) => {
        handle[eye, (Int, Int)](
          transformation,
          { trans => (evaluate[Double](trans.numRows).toInt, evaluate[Double](trans.numCols).toInt)},
          { case (_, (rows, cols)) => 
              val result = new SparseMatrix(rows, cols);
              result.viewDiagonal().assign(1);
              result
          }
        )
      }
      
      case (transformation: zeros) => {
        handle[zeros, (Int, Int)](
            transformation,
            {transformation => (evaluate[Double](transformation.numRows).toInt, 
                evaluate[Double](transformation.numCols).toInt)},
            { case (_, (rows, cols)) => 
              new SparseMatrix(rows, cols);  
            })
      }

      case (transformation: randn) => {

        handle[randn, (Int, Int, Double, Double)](transformation,
          { transformation =>
              (evaluate[Double](transformation.numRows).toInt, evaluate[Double](transformation.numColumns).toInt,
                  evaluate[Double](transformation.mean), evaluate[Double](transformation.std)) },
          { case (transformation, (numRows, numColumns, mean, std)) => {
              new DenseMatrix(numRows, numColumns).assign(new Normal(mean, std)) }})
      }
      
      case transformation: spones => {
        handle[spones, MahoutMatrix](transformation,
            { transformation => evaluate[MahoutMatrix](transformation.matrix) },
            { (_, matrix) => matrix.assign(CellwiseFunctions.binarize) })
      }

      //TODO remove this
      case transformation: sum => {
        handle[sum, (MahoutMatrix, Int)](transformation,
            { transformation => (evaluate[MahoutMatrix](transformation.matrix),
                evaluate[Double](transformation.dimension).toInt) },
            { case (_, (matrix, 2)) => {
                new DenseMatrix(matrix.numRows(),1).assignColumn(0, matrix.aggregateRows(VectorFunctions.sum))
              }
              case (_, (matrix, 1)) => {
                new DenseMatrix(1,matrix.numCols()).assignRow(0, matrix.aggregateColumns(VectorFunctions.sum))
            }})
      }
      
      case transformation: sumRow => {
        handle[sumRow, MahoutMatrix](transformation,
            { transformation => evaluate[MahoutMatrix](transformation.matrix) },
            { (_, matrix) => {
              new DenseMatrix(matrix.numRows(), 1).assignColumn(0, matrix.aggregateRows(VectorFunctions.sum))
            }})
      }
      
      case transformation: sumCol => {
        handle[sumCol, MahoutMatrix](transformation,
            { transformation => evaluate[MahoutMatrix](transformation.matrix) },
            { (_, matrix) => {
              new DenseMatrix(1, matrix.numCols()).assignRow(0, matrix.aggregateColumns(VectorFunctions.sum))
            }})
      }

      //TODO substitute with specialized operators
      case transformation: diag => {
        handle[diag, MahoutMatrix](transformation,
            {transformation => evaluate[MahoutMatrix](transformation.matrix)},
            { (_, matrix) => {
              (matrix.numRows, matrix.numCols) match {
                case (1, x) => new DiagonalMatrix(matrix.viewRow(0))
                case (x, 1) => new DiagonalMatrix(matrix.viewColumn(0))
                case (x,y) => {
                  val minimum = math.min(x,y)
                  new DenseMatrix(minimum, 1).assignColumn(0, matrix.viewDiagonal())
                }
              }
            }})
      }

      case (transformation: WriteMatrix) => {

        handle[WriteMatrix, MahoutMatrix](transformation,
          { transformation => evaluate[MahoutMatrix](transformation.matrix) },
          { (_, matrix) => println(matrix) })
      }
      
      case transformation: WriteString => {

        handle[WriteString, String](transformation,
            { transformation => evaluate[String](transformation.string) },
            { (_, string) => println(string) })
      }
      
      case transformation: WriteFunction => {
        handle[WriteFunction, Unit](transformation,
            { _ => },
            { (transformation, _) => PlanPrinter.print(transformation.function) })
      }
      

      case (transformation: scalar) => {

        handle[scalar, Unit](transformation,
          { _ => },
          { (transformation, _) => transformation.value })
      }

      case transformation: string => {
        handle[string, Unit](transformation,
          { _ => },
          { (transformation, _) => transformation.value })
      }

      case (transformation: WriteScalar) => {

        handle[WriteScalar, Double](transformation,
          { transformation => evaluate[Double](transformation.scalar) },
          { (_, scalar) => println(scalar) })
      }

      case transformation: UnaryScalarTransformation =>
        handle[UnaryScalarTransformation, Double](transformation,
          { transformation => evaluate[Double](transformation.scalar) },
          { (transformation, value) =>
            transformation.operation match {
              case Minus => -value
              case Binarize => CellwiseFunctions.binarize(value)
            }
          })

      case transformation: ScalarScalarTransformation => {
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
              case And => left && right
              case Or => left || right
              case Maximum => math.max(left, right)
              case Minimum => math.min(left, right)
              }
              result
          })
      }
      
      case transformation: Parameter => {
        throw new ExecutionRuntimeError("Parameters cannot be executed")
      }
      
      case transformation: function => {
        throw new ExecutionRuntimeError("Functions cannot be executed")
      }
    }

  }
}

