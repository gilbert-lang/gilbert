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

package org.gilbertlang.runtime.shell

import org.gilbertlang.runtime.Executables._

object printPlan {

  def apply(executable: Executable) = {
    PlanPrinter.print(executable)
  }
}

object PlanPrinter {

  def print(executable: Executable, depth: Int = 0): Unit = {

    executable match {

      case op: LoadMatrix => {
        printIndented(depth, op, "LoadMatrix [" + op.path + "]")
      }

      case op: FixpointIteration => {
        printIndented(depth, op, "FixpointIteration")
        print(op.initialState, depth + 1)
        print(op.updatePlan, depth + 1)
      }

      case op: FixpointIterationCellArray => {
        printIndented(depth, op, "FixpointIterationCellArray")
        print(op.initialState, depth + 1)
        print(op.updatePlan, depth + 1)
      }

      case IterationStatePlaceholder => {
        printIndented(depth, IterationStatePlaceholder, "IterationState")
      }

      case op: IterationStatePlaceholderCellArray => {
        printIndented(depth, op, "IterationStateCellArray")
      }

      case ConvergenceCurrentStatePlaceholder => {
        printIndented(depth, ConvergenceCurrentStatePlaceholder, "ConvergenceCurrentState")
      }

      case ConvergencePreviousStatePlaceholder => {
        printIndented(depth, ConvergencePreviousStatePlaceholder, "ConvergencePreviousState")
      }

      case placeholder: ConvergenceCurrentStateCellArrayPlaceholder => {
        printIndented(depth, placeholder, "ConvergenceCurrentStateCellArray")
      }

      case placeholder: ConvergencePreviousStateCellArrayPlaceholder => {
        printIndented(depth, placeholder, "ConvergencePreviousStateCellArray")
      }

      case op: CellwiseMatrixTransformation => {
        printIndented(depth, op, "CellwiseMatrixOp [" + op.operation + "]")
        print(op.matrix, depth + 1)
      }

      case op: CellwiseMatrixMatrixTransformation => {
        printIndented(depth, op, "CellwiseMatrixMatrixTransformation [" + op.operation + "]")
        print(op.left, depth + 1)
        print(op.right, depth + 1)
      }

      case op: Transpose => {
        printIndented(depth, op, "Transpose")
        print(op.matrix, depth + 1)
      }

      case op: MatrixMult => {
        printIndented(depth, op, "MatrixMult")
        print(op.left, depth + 1)
        print(op.right, depth + 1)
      }

      case op: AggregateMatrixTransformation => {
        printIndented(depth, op, "AggregateMatrixOp [" + op.operation + "]")
        print(op.matrix, depth + 1)
      }

      case op: VectorwiseMatrixTransformation => {
        printIndented(depth, op, "VectorwiseMatrixTransformation [" + op.operation + "]")
        print(op.matrix, depth + 1)
      }

      case (op: ScalarMatrixTransformation) => {
        printIndented(depth, op, "ScalarMatrixOp [" + op.operation + "]")
        print(op.scalar, depth + 1)
        print(op.matrix, depth + 1)
      }

      case op: MatrixScalarTransformation => {
        printIndented(depth, op, "MatrixScalarOp [" + op.operation + "]")
        print(op.matrix, depth + 1)
        print(op.scalar, depth + 1)
      }
      
      case op: ScalarScalarTransformation => {
        printIndented(depth, op, "ScalarScalarOp [" + op.operation + "]")
        print(op.left, depth + 1)
        print(op.right, depth +1 )
      }
      
      case op: UnaryScalarTransformation => {
        printIndented(depth, op, "UnaryScalarOp [" + op.operation + "]")
        print(op.scalar, depth+1)
      }

      case op: ones => {
        printIndented(depth, op, "ones")
        print(op.numRows, depth+1)
        print(op.numColumns, depth+1)
      }

      case op: randn => {
        printIndented(depth, op, "rand")
        print(op.numRows, depth + 1)
        print(op.numColumns, depth + 1)
        print(op.mean, depth + 1)
        print(op.std, depth + 1)
      }
      
      case op: spones => {
        printIndented(depth, op, "spones")
        print(op.matrix, depth +  1)
      }
      
      case op: sum => {
        printIndented(depth, op, "sum")
        print(op.matrix, depth + 1)
        print(op.dimension, depth + 1)
      }
      
      case op: sumRow => {
        printIndented(depth, op, "sumRow")
        print(op.matrix,depth+1)
      }
      
      case op: sumCol => {
        printIndented(depth, op, "sumCol")
        print(op.matrix,depth + 1)
      }
      
      case op: diag => {
        printIndented(depth, op, "diag")
        print(op.matrix,depth + 1)
      }

      case op: WriteMatrix => {
        printIndented(depth, op, "WriteMatrix")
        print(op.matrix, depth + 1)
      }

      case op: scalar => {
        printIndented(depth, op, op.value.toString)
      }
      
      case op: string => {
        printIndented(depth, op, op.value)
      }
      
      case op:function => {
        printIndented(depth, op, "Function(" + op.numParameters + ")")
        print(op.body, depth + 1)
      }

      case op: boolean => {
        printIndented(depth, op, op.value.toString)
      }

      case op: WriteScalar => {
        printIndented(depth, op, "WriteScalarRef")
        print(op.scalar, depth + 1)
      }
      
      case op: WriteString => {
        printIndented(depth, op, "WriteString")
        print(op.string, depth + 1)
      }
      
      case op: WriteFunction => {
        printIndented(depth, op, "WriteFunction")
        print(op.function, depth + 1)
      }

      case op:WriteCellArray => {
        printIndented(depth, op, "WriteCellArray")
        print(op.cellArray, depth+1)
      }

      case op: CompoundExecutable => {
        printIndented(depth, op, "CompoundExecutable(")
        op.executables foreach { print(_, depth + 1) }
        printIndented(depth, "", ")")
      }
      
      case op:Parameter => {
        printIndented(depth, op, "Parameter(" + op.position + ")")
      }

      
      case VoidExecutable => {
        printIndented(depth, VoidExecutable, "VoidExecutable")
      }
      
      case op: zeros => {
        printIndented(depth, op, "zeros")
        print(op.numRows, depth+1)
        print(op.numCols, depth+1)
      }
      
      case op: eye => {
        printIndented(depth, op, "eye")
        print(op.numRows, depth+1)
        print(op.numCols, depth+1)
      }

      case op: norm => {
        printIndented(depth, op, "norm")
        print(op.matrix, depth+1)
        print(op.p, depth+1)
      }

      case op: CellArrayReferenceMatrix => {
        printIndented(depth, op, "CellArrayReferenceMatrix")
        print(op.parent, depth+1)
        print(scalar(op.reference), depth+1)
      }

      case op: CellArrayReferenceCellArray => {
        printIndented(depth, op, "CellArrayReferenceCellArray")
        print(op.parent, depth+1)
        print(scalar(op.reference), depth+1)
      }

      case op: CellArrayReferenceFunction => {
        printIndented(depth, op, "CellArrayReferenceFunction")
        print(op.parent, depth+1)
        print(scalar(op.reference), depth+1)
      }

      case op: CellArrayReferenceScalar => {
        printIndented(depth, op, "CellArrayReferenceScalar")
        print(op.parent, depth+1)
        print(scalar(op.reference), depth+1)
      }

      case op: CellArrayReferenceString => {
        printIndented(depth, op, "CellArrayReferenceString")
        print(op.parent, depth+1)
        print(scalar(op.reference), depth+1)
      }

      case op: CellArrayExecutable => {
        printIndented(depth, op, "CellArrayExecutable")
        op.elements foreach { print(_, depth+1)}
      }
    }

  }

  def printIndented(depth: Int, transformation: Executable, str: String) = {
    println("".padTo(depth, "  ").mkString + "(" + transformation.id + ") " + str)
  }
}
