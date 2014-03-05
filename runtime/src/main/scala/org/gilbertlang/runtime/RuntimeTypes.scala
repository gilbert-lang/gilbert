package org.gilbertlang.runtime


object RuntimeTypes {
  sealed trait RuntimeType
  case object Undefined extends RuntimeType
  case object Void extends RuntimeType
  sealed trait ScalarType extends RuntimeType
  case object ScalarType extends ScalarType
  case object BooleanType extends ScalarType
  case object DoubleType extends ScalarType
  case object StringType extends RuntimeType
  case object FunctionType extends RuntimeType
  case object Unknown extends RuntimeType
  case class MatrixType(elementType: RuntimeType) extends RuntimeType
  case class CellArrayType(elementTypes: List[RuntimeType]) extends RuntimeType
}
