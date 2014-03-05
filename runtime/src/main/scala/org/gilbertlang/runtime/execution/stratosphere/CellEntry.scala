package org.gilbertlang.runtime.execution.stratosphere


case class CellEntry(index: Int, value: ValueWrapper){
  def wrappedValue[T]: T = value.value.asInstanceOf[T]
}
