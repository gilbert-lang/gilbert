package org.gilbertlang.runtime


object RuntimeTypes {
  sealed trait RuntimeType
  case object Undefined extends RuntimeType
  case object Void extends RuntimeType
  case object BooleanType extends RuntimeType
  case object DoubleType extends RuntimeType
  case object StringType extends RuntimeType
  case object FunctionType extends RuntimeType
  case object Unknown extends RuntimeType
  case class MatrixType(elementType: RuntimeType) extends RuntimeType
}
