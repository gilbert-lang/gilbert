package org.gilbertlang.runtime

class RuntimeError(msg: String) extends Error(msg) {
  def this() = this("Runtime error")
}

class InstantiationRuntimeError(msg: String) extends RuntimeError(msg) {
  def this() = this("Instation error")
}

class ExecutionRuntimeError(msg: String) extends RuntimeError(msg) {
  def this() = this("Execution runtime error")
}