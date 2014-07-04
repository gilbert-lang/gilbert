package org.gilbertlang.evaluation

object Engines extends Enumeration {
  val Stratosphere = Value("Stratosphere")
  val Spark = Value("Spark")
  val Local = Value("Local")
}
