package org.gilbertlang.runtime.execution.flink


case class CellEntry(index: Int, value: ValueWrapper){
  def wrappedValue[T]: T = value.value.asInstanceOf[T]
}
