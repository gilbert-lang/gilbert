package org.gilbertlang.runtimeMacros.linalg


trait PartitionPlan extends Iterable[Partition]{
  protected def maxId: Int
  def maxRowIndex: Int
  def maxColumnIndex: Int
  def partitionId(row: Int, column: Int):Int
  def getPartition(id: Int): Partition
  def getPartition(rowIndex: Int, columnIndex: Int): Partition
  
  override def iterator: Iterator[Partition] = {
    new Iterator[Partition]{
      var counter = 0;
      
      override def hasNext: Boolean = counter < maxId
      
      override def next(): Partition = {
        val result = getPartition(counter)
        counter += 1
        result
      }
    }
  }
}