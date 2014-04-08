package org.gilbertlang.runtime.execution.spark

case class CellEntry(idx: Int, value: Any) extends Serializable{
  override def toString = value.toString
}