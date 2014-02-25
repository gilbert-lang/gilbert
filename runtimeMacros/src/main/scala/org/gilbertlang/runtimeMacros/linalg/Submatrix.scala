package org.gilbertlang.runtimeMacros.linalg

import breeze.linalg.{ Matrix => BreezeMatrix, MatrixLike => BreezeMatrixLike, Vector => BreezeVector, DenseMatrix => BreezeDenseMatrix }
import org.gilbertlang.runtimeMacros.linalg.operators.SubmatrixOps
import breeze.linalg.support.CanTraverseValues
import CanTraverseValues.ValuesVisitor
import breeze.linalg.support.CanMapValues
import breeze.linalg.support.CanZipMapValues
import breeze.linalg.support.CanSlice2
import breeze.linalg.support.CanCollapseAxis
import breeze.linalg.support.CanCopy
import breeze.linalg.Axis
import scala.util.Random
import org.gilbertlang.runtimeMacros.linalg.io.DataWriter
import org.gilbertlang.runtimeMacros.linalg.io.DataReader
import scala.reflect.ClassTag
import breeze.storage.DefaultArrayValue
import breeze.math.Semiring

case class Submatrix[@specialized(Double, Boolean) T:DataWriter:DataReader:ClassTag:Semiring:DefaultArrayValue] 
(var matrix: GilbertMatrix[T], var rowIndex: Int, var columnIndex: Int, var rowOffset: Int,
  var columnOffset: Int, var totalRows: Int, var totalColumns: Int) extends BreezeMatrix[T] with BreezeMatrixLike[T, Submatrix[T]] {

  override def rows = matrix.rows
  override def cols = matrix.cols

  override def apply(i: Int, j: Int): T = matrix(i, j)

  override def copy = breeze.linalg.copy(this)

  override def update(i: Int, j: Int, value: T) = matrix.update(i, j, value)

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
      s"Submatrix[$index $size $offset $totalSize]" + "\n" + matrix
    } else {
      s"Submatrix[$index $offset $totalSize]"
    }
  }
  
  def getPartition = Partition(-1, rowIndex, columnIndex, rows, cols, rowOffset, columnOffset, totalRows, totalColumns)
}

object Submatrix extends SubmatrixOps {
    
  implicit def canCopySubmatrix[@specialized(Double, Boolean) T: DataWriter: DataReader: ClassTag: Semiring: 
  DefaultArrayValue]: CanCopy[Submatrix[T]] = new CanCopy[Submatrix[T]]{
    override def apply(submatrix: Submatrix[T]): Submatrix[T] = {
      import submatrix._
      Submatrix[T](breeze.linalg.copy(submatrix.matrix), rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
    }
  }
    def apply[@specialized(Double, Boolean) T:DataWriter:DataReader:ClassTag:DefaultArrayValue:Semiring:MatrixFactory]
    (partitionInformation: Partition, entries: Seq[(Int,Int,T)]): Submatrix[T] = {
      import partitionInformation._
      val adjustedEntries = entries map { case (row, col, value) => (row - rowOffset, col - columnOffset, value)}
      val gilbertMatrix = GilbertMatrix(numRows, numColumns, adjustedEntries)
      Submatrix(gilbertMatrix, rowIndex, columnIndex, rowOffset,columnOffset, numTotalRows, numTotalColumns)
    }
  
    def apply[@specialized(Double, Boolean) T:DataWriter:DataReader:ClassTag:DefaultArrayValue:Semiring:MatrixFactory]
    (partitionInformation: Partition, numNonZeroElements: Int = 0): Submatrix[T] = {
      import partitionInformation._
      Submatrix(GilbertMatrix(numRows, numColumns, numNonZeroElements), rowIndex, columnIndex, rowOffset,
          columnOffset, numTotalRows, numTotalColumns)
    }
    
    def init[@specialized(Double, Boolean) T:DataWriter:DataReader:ClassTag:DefaultArrayValue:Semiring:MatrixFactory]
    (partitionInformation: Partition, initialValue: T): Submatrix[T] = {
      Submatrix(GilbertMatrix.init(partitionInformation.numRows, partitionInformation.numColumns, initialValue),
          partitionInformation.rowIndex, partitionInformation.columnIndex, partitionInformation.rowOffset,
          partitionInformation.columnOffset, partitionInformation.numTotalRows, partitionInformation.numTotalColumns)
    }
    
    def eye[@specialized(Double, Boolean) T:DataWriter:DataReader:ClassTag:DefaultArrayValue:Semiring:MatrixFactory]
    (partitionInformation: Partition): Submatrix[T] = {
      import partitionInformation._
      Submatrix(GilbertMatrix.eye(numRows, numColumns), rowIndex, columnIndex, rowOffset, columnOffset, numTotalRows,
          numTotalColumns)
    }
    
    def rand(partition: Partition, random: Random = new Random()): Submatrix[Double] = {
      import partition._
      Submatrix(GilbertMatrix.rand(numRows, numColumns, random), rowIndex, columnIndex, rowOffset, columnOffset,
          numTotalRows, numTotalColumns)
    }
  
  def outputFormatter[@specialized(Double, Boolean) T](elementDelimiter: String, fieldDelimiter: String) = {
    new Function1[Submatrix[T], String] {
      def apply(submatrix: Submatrix[T]): String = {
        var result = "";
        for (((row, col), value) <- submatrix.activeIterator) {
          result += ((row+1)+submatrix.rowOffset) + fieldDelimiter + ((col+1)+submatrix.columnOffset) + fieldDelimiter + 
          value + elementDelimiter

        }
        result
      }
    }
  }
  
  implicit def canMapValues[@specialized(Double, Boolean)  T:DataWriter:DataReader:ClassTag:Semiring:DefaultArrayValue] = { 
    new CanMapValues[Submatrix[T], T, T, Submatrix[T]]{
      override def map(submatrix: Submatrix[T], fun: T => T) = {
        val gilbert:GilbertMatrix[T] = submatrix.matrix.map(fun)
        import submatrix._
        Submatrix[T](gilbert, rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
      }
      
      override def mapActive(submatrix: Submatrix[T], fun: T => T) = {
        Submatrix(submatrix.matrix.mapActiveValues(fun), submatrix.rowIndex, submatrix.columnIndex, submatrix.rowOffset,
            submatrix.columnOffset, submatrix.totalRows, submatrix.totalColumns)
      }
    }
  }
  
  implicit def handholdCMV[T] = new CanMapValues.HandHold[Submatrix[T], T]
  
  implicit def canZipMapValues[@specialized(Double, Boolean) T:DataWriter:DataReader:ClassTag:Semiring:
    DefaultArrayValue]: CanZipMapValues[Submatrix[T], T, T, Submatrix[T]] = {
    new CanZipMapValues[Submatrix[T], T, T, Submatrix[T]]{
      override def map(a: Submatrix[T], b: Submatrix[T], fn: (T, T) => T) = {
        val mapper = implicitly[CanZipMapValues[GilbertMatrix[T], T, T, GilbertMatrix[T]]]
        val result = mapper.map(a.matrix, b.matrix, fn)
        Submatrix(result, a.rowIndex, a.columnIndex, a.rowOffset, a.columnOffset, a.totalRows, a.totalColumns)
      }
    }
  }
  
  implicit def canIterateValues[@specialized(Double, Boolean) T:DataWriter:DataReader:ClassTag:Semiring:
    DefaultArrayValue]: CanTraverseValues[Submatrix[T], T] = {
    new CanTraverseValues[Submatrix[T], T]{
      override def isTraversableAgain(submatrix: Submatrix[T]): Boolean = true
      
      override def traverse(submatrix: Submatrix[T], fn: ValuesVisitor[T]) {
        val traversal = implicitly[CanTraverseValues[GilbertMatrix[T], T]]
        traversal.traverse(submatrix.matrix, fn)
      }
    }
  }
  
  implicit def canSliceRowSubmatrix[@specialized(Double, Boolean) T:DataWriter:DataReader:ClassTag:Semiring:
    DefaultArrayValue]: CanSlice2[Submatrix[T], Int, ::.type, Submatrix[T]] = {
    new CanSlice2[Submatrix[T], Int, ::.type, Submatrix[T]]{
      override def apply(submatrix: Submatrix[T], row: Int, ignored: ::.type) = {
        Submatrix(submatrix.matrix(row, ::), submatrix.rowIndex, submatrix.columnIndex, submatrix.rowOffset,
            submatrix.columnOffset, submatrix.totalRows, submatrix.totalColumns)
      }
    }
  }
  
  implicit def canSliceRowsSubmatrix[@specialized(Double, Boolean) T:DataWriter:DataReader:ClassTag:Semiring:
    DefaultArrayValue]: CanSlice2[Submatrix[T], Range, ::.type, Submatrix[T]] = {
    new CanSlice2[Submatrix[T], Range, ::.type, Submatrix[T]]{
      override def apply(submatrix: Submatrix[T], rows: Range, ignored: ::.type) = {
        Submatrix(submatrix.matrix(rows, ::), submatrix.rowIndex, submatrix.columnIndex, submatrix.rowOffset,
            submatrix.columnOffset, submatrix.totalRows, submatrix.totalColumns)
      }
    }
  }
  
  implicit def canSliceColSubmatrix[@specialized(Double, Boolean) T:DataWriter:DataReader:ClassTag:Semiring:
    DefaultArrayValue]: CanSlice2[Submatrix[T], ::.type, Int, Subvector[T]] = {
    new CanSlice2[Submatrix[T], ::.type, Int, Subvector[T]]{
      override def apply(submatrix: Submatrix[T], ignored: ::.type, col: Int) ={
        Subvector(submatrix.matrix(::, col), submatrix.rowIndex, submatrix.rowOffset, submatrix.totalRows)
      }
    }
  }
  
  implicit def canSliceColsSubmatrix[@specialized(Double, Boolean) T:DataWriter:DataReader:ClassTag:Semiring:
    DefaultArrayValue]: CanSlice2[Submatrix[T], ::.type, Range, Submatrix[T]] = {
      new CanSlice2[Submatrix[T], ::.type, Range, Submatrix[T]]{
        override def apply(submatrix: Submatrix[T], ignored: ::.type, cols: Range) = {
          Submatrix(submatrix.matrix(::, cols), submatrix.rowIndex, submatrix.columnIndex, submatrix.rowOffset, 
              submatrix.columnOffset, submatrix.totalRows, submatrix.totalColumns)
        }
      }
  }
  
  implicit def canCollapseRows[@specialized(Double, Boolean) T:DataWriter:DataReader:ClassTag:Semiring:
    DefaultArrayValue]: CanCollapseAxis[Submatrix[T], Axis._0.type, BreezeVector[T], T, Submatrix[T]] = {
    new CanCollapseAxis[Submatrix[T], Axis._0.type, BreezeVector[T], T, Submatrix[T]]{
      override def apply(submatrix: Submatrix[T], axis: Axis._0.type)(fn: BreezeVector[T] => T) = {
        val collapser = 
          implicitly[CanCollapseAxis[GilbertMatrix[T], Axis._0.type, BreezeVector[T], T, GilbertMatrix[T]]]
        Submatrix(collapser(submatrix.matrix, axis)(fn), 0, submatrix.columnIndex, 0, submatrix.columnOffset,
            1, submatrix.totalColumns)
      }
    }
  }
  
  implicit def canCollapseCols[@specialized(Double, Boolean) T:DataWriter:DataReader:ClassTag:Semiring:
    DefaultArrayValue]: CanCollapseAxis[Submatrix[T], Axis._1.type, BreezeVector[T], T, Subvector[T]] = {
    new CanCollapseAxis[Submatrix[T], Axis._1.type, BreezeVector[T], T, Subvector[T]]{
      override def apply(submatrix: Submatrix[T], axis: Axis._1.type)(fn: BreezeVector[T] => T) = {
        val collapser = 
          implicitly[CanCollapseAxis[GilbertMatrix[T], Axis._1.type, BreezeVector[T], T, GilbertVector[T]]]
        Subvector(collapser(submatrix.matrix, axis)(fn), submatrix.rowIndex, submatrix.rowOffset, submatrix.totalRows)
      }
    }
  }
  
  implicit def handholdCanMapCols[T]: CanCollapseAxis.HandHold[Submatrix[T], Axis._1.type, BreezeVector[T]] = 
    new CanCollapseAxis.HandHold[Submatrix[T], Axis._1.type, BreezeVector[T]]()
    
  implicit def handholdCanMapRows[T]: CanCollapseAxis.HandHold[Submatrix[T], Axis._0.type, BreezeVector[T]] = 
    new CanCollapseAxis.HandHold[Submatrix[T], Axis._0.type, BreezeVector[T]]()

}

