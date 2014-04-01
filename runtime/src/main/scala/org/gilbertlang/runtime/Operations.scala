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

package org.gilbertlang.runtime

object Operations {

  sealed trait CellwiseOperation
  sealed trait ScalarsOperation extends CellwiseOperation
  sealed trait ScalarMatrixOperation extends CellwiseOperation

  sealed trait ArithmeticOperation extends ScalarsOperation with ScalarMatrixOperation
  case object Addition extends ArithmeticOperation
  case object Subtraction extends ArithmeticOperation
  case object Multiplication extends ArithmeticOperation
  case object Division extends ArithmeticOperation
  case object Exponentiation extends ArithmeticOperation

  sealed trait ComparisonOperation extends ScalarsOperation with ScalarMatrixOperation
  case object GreaterThan extends ComparisonOperation
  case object GreaterEqualThan extends ComparisonOperation
  case object LessThan extends ComparisonOperation
  case object LessEqualThan extends ComparisonOperation
  case object Equals extends ComparisonOperation
  case object NotEquals extends ComparisonOperation
  
  sealed trait LogicOperation extends ScalarsOperation with ScalarMatrixOperation
  case object And extends LogicOperation
  case object Or extends LogicOperation
  case object SCAnd extends LogicOperation
  case object SCOr extends LogicOperation

  //TODO do we need this on vectors?
  sealed trait MinMax extends ScalarsOperation with VectorwiseOperation with AggregateMatrixOperation
  case object Maximum extends MinMax
  case object Minimum extends MinMax

  sealed trait UnaryScalarOperation
  //TODO remove
  case object Minus extends UnaryScalarOperation
  case object Binarize extends UnaryScalarOperation

  sealed trait VectorwiseOperation
  case object NormalizeL1 extends VectorwiseOperation

  sealed trait AggregateMatrixOperation
  case object SumAll extends AggregateMatrixOperation

  sealed trait NormOperation extends AggregateMatrixOperation with VectorwiseOperation
  case object Norm2 extends NormOperation


}