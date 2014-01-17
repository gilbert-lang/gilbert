package org.gilbertlang.runtime.execution.stratosphere

class TransformationNotSupportedError(msg: String) extends Error(msg){
  def this() = this("Transformation not supported.")
}

class IllegalArgumentError(msg: String) extends Error(msg) {
  def this() = this("Illegal Argument error.")
}