package org.gilbertlang.runtime.execution.flink

class TransformationNotSupportedError(msg: String) extends Error(msg){
  def this() = this("Transformation not supported.")
}

class IllegalArgumentError(msg: String) extends Error(msg) {
  def this() = this("Illegal Argument error.")
}

class StratosphereExecutionError(msg: String) extends Error(msg) {
  def this() = this("Stratosphere execution error")
}