package org.gilbertlang.evaluation

object Engines extends Enumeration {
  val Flink = Value("Flink")
  val Spark = Value("Spark")
  val Local = Value("Local")
}
