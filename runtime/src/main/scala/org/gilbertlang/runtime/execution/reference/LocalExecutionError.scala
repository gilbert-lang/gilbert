package org.gilbertlang.runtime.execution.reference

/**
 * Created by till on 02/04/14.
 */
class LocalExecutionError(msg: String) extends Error(msg) {
  def this() = this("Local execution error")
}
