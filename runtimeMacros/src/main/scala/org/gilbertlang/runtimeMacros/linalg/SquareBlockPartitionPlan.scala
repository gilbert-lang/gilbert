package org.gilbertlang.runtimeMacros.linalg

case class SquareBlockPartitionPlan(blockSize: Int, totalRows: Int, totalColumns: Int)
  extends PartitionPlan{
  
  val maxRowIndex = math.ceil(totalRows.toDouble/blockSize).toInt
  val maxColumnIndex = math.ceil(totalColumns.toDouble/blockSize).toInt
  
  def maxId = maxRowIndex*maxColumnIndex
  
  def partitionId(row: Int, column: Int) = {
    (row / blockSize)*maxColumnIndex + column / blockSize
  }
  
  def getPartition(id: Int) = {
    val (rowIndex, columnIndex) = partitionIndices(id)
    val (numRows, numColumns) = partitionSize(id)
    val(rowOffset, columnOffset) = partitionOffset(id)
    Partition(id, rowIndex, columnIndex, numRows, numColumns, rowOffset, columnOffset, totalRows, totalColumns)
  }
  
  def getPartition(rowIndex: Int, columnIndex: Int) = {
    val id = rowIndex * maxColumnIndex + columnIndex
    val (numRows, numColumns) = partitionSize(id)
    val(rowOffset, columnOffset) = partitionOffset(id)
    Partition(id, rowIndex, columnIndex, numRows, numColumns, rowOffset, columnOffset, totalRows, totalColumns)
  }
  
  def partitionSize(partitionId: Int) = {
    val (rowId, columnId) = partitionIndices(partitionId)
    
    val rowSize = if(rowId < maxRowIndex-1) blockSize else totalRows - blockSize*rowId
    val columnSize=  if(columnId < maxColumnIndex-1) blockSize else totalColumns - blockSize*columnId
    
    (rowSize, columnSize)
  }
  
  def partitionOffset(partitionId: Int) = {
    val (rowId, columnId) = partitionIndices(partitionId)
    
    val rowStart = rowId * blockSize
    val columnStart = columnId * blockSize
    
    (rowStart, columnStart)
  }
  
  def partitionIndices(partitionId: Int) = {
    (partitionId/maxColumnIndex, partitionId % maxColumnIndex)
  }
}