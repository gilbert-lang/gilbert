package org.gilbertlang.runtimeMacros.linalg

import breeze.linalg.{ Matrix => BreezeMatrix, MatrixLike => BreezeMatrixLike, Vector => BreezeVector, DenseMatrix => BreezeDenseMatrix }
import org.gilbertlang.runtimeMacros.linalg.operators.SubmatrixBooleanOps
import breeze.linalg.support.CanTraverseValues
import CanTraverseValues.ValuesVisitor
import breeze.linalg.support.CanMapValues
import breeze.linalg.support.CanZipMapValues
import breeze.linalg.support.CanSlice2
import breeze.linalg.support.CanCollapseAxis
import breeze.linalg.support.CanCopy
import breeze.linalg.Axis
import scala.util.Random
import scala.reflect.ClassTag
import breeze.storage.DefaultArrayValue
import breeze.math.Semiring

case class SubmatrixBoolean(var matrix: GilbertMatrixBoolean, var rowIndex: Int, var columnIndex: Int, var rowOffset: Int,
  var columnOffset: Int, var totalRows: Int, var totalColumns: Int) extends BreezeMatrix[Boolean] with 
  BreezeMatrixLike[Boolean, SubmatrixBoolean] {

  override def rows = matrix.rows
  override def cols = matrix.cols

  override def apply(i: Int, j: Int): Boolean = matrix(i, j)

  override def copy = new SubmatrixBoolean(this.matrix.copy, rowIndex, columnIndex,
      rowOffset, columnOffset, totalRows, totalColumns)

  override def update(i: Int, j: Int, value: Boolean) = matrix.update(i, j, value)

  override def repr = this

  def activeIterator = matrix.activeIterator
  def activeKeysIterator = matrix.activeKeysIterator
  def activeValuesIterator = matrix.activeValuesIterator

  def activeSize = matrix.activeSize

  def this() = this(null, -1, -1, -1, -1, -1, -1)

  override def toString: String = {
    val index = s"Index: ($rowIndex, $columnIndex)"
    val offset = s"Offset: ($rowOffset, $columnOffset)"
    val totalSize = s"Total size: ($totalRows, $totalColumns)"

    if (matrix != null) {
      val size = s"Size: ($rows, $cols)"
      s"SubmatrixBoolean[$index $size $offset $totalSize]" + "\n" + matrix
    } else {
      s"SubmatrixBoolean[$index $offset $totalSize]"
    }
  }
  
  def getPartition = Partition(-1, rowIndex, columnIndex, rows, cols, rowOffset, columnOffset, totalRows, totalColumns)
}

object SubmatrixBoolean extends SubmatrixBooleanOps {
    
  implicit def canCopySubmatrixBoolean:CanCopy[SubmatrixBoolean] = new CanCopy[SubmatrixBoolean]{
    override def apply(submatrix: SubmatrixBoolean): SubmatrixBoolean = {
      import submatrix._
      SubmatrixBoolean(breeze.linalg.copy(submatrix.matrix), rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
    }
  }
    def apply(partitionInformation: Partition, entries: Seq[(Int,Int,Boolean)]): SubmatrixBoolean = {
      import partitionInformation._
      val adjustedEntries = entries map { case (row, col, value) => (row - rowOffset, col - columnOffset, value)}
      val gilbertMatrix = GilbertMatrixBoolean(numRows, numColumns, adjustedEntries)
      SubmatrixBoolean(gilbertMatrix, rowIndex, columnIndex, rowOffset,columnOffset, numTotalRows, numTotalColumns)
    }
  
    def apply(partitionInformation: Partition, numNonZeroElements: Int = 0): SubmatrixBoolean = {
      import partitionInformation._
      SubmatrixBoolean(GilbertMatrixBoolean(numRows, numColumns, numNonZeroElements), rowIndex, columnIndex, rowOffset,
          columnOffset, numTotalRows, numTotalColumns)
    }
    
    def init(partitionInformation: Partition, initialValue: Boolean): SubmatrixBoolean = {
      SubmatrixBoolean(GilbertMatrixBoolean.init(partitionInformation.numRows, partitionInformation.numColumns, initialValue),
          partitionInformation.rowIndex, partitionInformation.columnIndex, partitionInformation.rowOffset,
          partitionInformation.columnOffset, partitionInformation.numTotalRows, partitionInformation.numTotalColumns)
    }
    
    def eye(partitionInformation: Partition): SubmatrixBoolean = {
      import partitionInformation._
      SubmatrixBoolean(GilbertMatrixBoolean.eye(numRows, numColumns), rowIndex, columnIndex, rowOffset, columnOffset, numTotalRows,
          numTotalColumns)
    }
  
  def outputFormatter(elementDelimiter: String, fieldDelimiter: String) = {
    new Function1[SubmatrixBoolean, String] {
      def apply(submatrix: SubmatrixBoolean): String = {
        var result = "";
        for (((row, col), value) <- submatrix.activeIterator) {
          result += ((row+1)+submatrix.rowOffset) + fieldDelimiter + ((col+1)+submatrix.columnOffset) + fieldDelimiter + 
          value + elementDelimiter

        }
        result
      }
    }
  }
}

