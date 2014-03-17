package org.gilbertlang.runtime


object RuntimeTypes {
  sealed trait RuntimeType
  case object Undefined extends RuntimeType
  case object Void extends ScalarType with FunctionType with AbstractMatrixType
  case object Unknown extends ScalarType with FunctionType with AbstractMatrixType
  sealed trait ScalarType extends RuntimeType
  case object ScalarType extends ScalarType
  case object BooleanType extends ScalarType
  case object DoubleType extends ScalarType
  case object StringType extends RuntimeType
  sealed trait FunctionType extends RuntimeType
  case object FunctionType extends FunctionType
  sealed trait AbstractMatrixType extends RuntimeType
  case class MatrixType(elementType: RuntimeType, rows: Option[Int], cols: Option[Int]) extends AbstractMatrixType
  case class CellArrayType(elementTypes: List[RuntimeType]) extends RuntimeType
}
