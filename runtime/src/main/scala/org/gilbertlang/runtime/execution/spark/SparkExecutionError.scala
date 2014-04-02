package org.gilbertlang.runtime.execution.spark

/**
 * Created by till on 03/04/14.
 */
class SparkExecutionError(msg: String) extends Error(msg) {
  def this() = this("Spark execution error")
}
