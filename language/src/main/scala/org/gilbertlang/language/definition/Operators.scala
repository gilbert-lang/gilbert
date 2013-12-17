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

package org.gilbertlang.language.definition

object Operators {

  sealed abstract class Operator
  
  sealed abstract class UnaryOperator extends Operator
  sealed abstract class CellwiseUnaryOperator extends UnaryOperator

  case object TransposeOp extends UnaryOperator
  case object CellwiseTransposeOp extends CellwiseUnaryOperator
  case object PrePlusOp extends CellwiseUnaryOperator
  case object PreMinusOp extends CellwiseUnaryOperator

  sealed abstract class BinaryOperator extends Operator
  sealed abstract class CellwiseBinaryOperator extends BinaryOperator
  
  case object ExpOp extends BinaryOperator
  case object PlusOp extends BinaryOperator
  case object MinusOp extends BinaryOperator
  case object MultOp extends BinaryOperator
  case object DivOp extends BinaryOperator
  
  case object BinaryAndOp extends BinaryOperator
  case object BinaryOrOp extends BinaryOperator
  case object LogicalAndOp extends BinaryOperator
  case object LogicalOrOp extends BinaryOperator
  case object GTOp extends BinaryOperator
  case object GTEOp extends BinaryOperator
  case object LTOp extends BinaryOperator
  case object LTEOp extends BinaryOperator
  case object DEQOp extends BinaryOperator
  
  case object CellwiseMultOp extends CellwiseBinaryOperator
  case object CellwiseDivOp extends CellwiseBinaryOperator
  case object CellwiseExpOp extends CellwiseBinaryOperator
  
  def op2Str(operator:Operator): String = {
    operator match {
      case TransposeOp => "'"
      case CellwiseTransposeOp => ".'"
      case PrePlusOp => "+"
      case PreMinusOp => "-"
      case ExpOp => "^"
      case PlusOp => "+"
      case MinusOp => "-"
      case MultOp => "*"
      case DivOp => "/"
      case BinaryAndOp => "&"
      case BinaryOrOp => "|"
      case LogicalAndOp => "&&"
      case LogicalOrOp => "||"
      case GTOp => ">"
      case GTEOp => ">="
      case LTOp => "<"
      case LTEOp => "<="
      case DEQOp => "=="
      case CellwiseMultOp => ".*"
      case CellwiseDivOp => "./"
      case CellwiseExpOp => ".^"
    }
  }
}