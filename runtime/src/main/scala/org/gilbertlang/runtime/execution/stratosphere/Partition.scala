package org.gilbertlang.runtime.execution.stratosphere

case class Partition(id: Int, rowIndex: Int, columnIndex: Int, numRows: Int, numColumns: Int, rowOffset: Int,
  columnOffset: Int, numTotalRows: Int, numTotalColumns: Int) {

}