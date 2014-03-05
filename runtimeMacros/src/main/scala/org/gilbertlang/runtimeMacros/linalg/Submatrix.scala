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
import scala.reflect.ClassTag
import breeze.storage.DefaultArrayValue
import breeze.math.Semiring
import eu.stratosphere.types.Value
import java.io.{DataInput, DataOutput}

case class Submatrix(var matrix: GilbertMatrix, var rowIndex: Int, var columnIndex: Int, var rowOffset: Int,
  var columnOffset: Int, var totalRows: Int, var totalColumns: Int) extends BreezeMatrix[Double] with 
  BreezeMatrixLike[Double, Submatrix] with Value {

  override def rows = matrix.rows
  override def cols = matrix.cols

  override def apply(i: Int, j: Int): Double = matrix(i, j)

  override def copy = new Submatrix(this.matrix.copy, rowIndex, columnIndex,
      rowOffset, columnOffset, totalRows, totalColumns)

  override def update(i: Int, j: Int, value: Double) = matrix.update(i, j, value)

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

  override def write(out: DataOutput){
    matrix.write(out)
    out.writeInt(rowIndex)
    out.writeInt(columnIndex)
    out.writeInt(rowOffset)
    out.writeInt(columnOffset)
    out.writeInt(totalRows)
    out.writeInt(totalColumns)
  }

  override def read(in: DataInput){
    matrix = new GilbertMatrix()
    matrix.read(in)
    rowIndex = in.readInt()
    columnIndex = in.readInt()
    rowOffset = in.readInt()
    columnOffset = in.readInt()
    totalRows = in.readInt()
    totalColumns = in.readInt()
  }
}

object Submatrix extends SubmatrixOps {
    
  implicit def canCopySubmatrix:CanCopy[Submatrix] = new CanCopy[Submatrix]{
    override def apply(submatrix: Submatrix): Submatrix = {
      import submatrix._
      Submatrix(breeze.linalg.copy(submatrix.matrix), rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
    }
  }
    def apply(partitionInformation: Partition, entries: Seq[(Int,Int,Double)]): Submatrix = {
      import partitionInformation._
      val adjustedEntries = entries map { case (row, col, value) => (row - rowOffset, col - columnOffset, value)}
      val gilbertMatrix = GilbertMatrix(numRows, numColumns, adjustedEntries)
      Submatrix(gilbertMatrix, rowIndex, columnIndex, rowOffset,columnOffset, numTotalRows, numTotalColumns)
    }
  
    def apply(partitionInformation: Partition, numNonZeroElements: Int = 0): Submatrix = {
      import partitionInformation._
      Submatrix(GilbertMatrix(numRows, numColumns, numNonZeroElements), rowIndex, columnIndex, rowOffset,
          columnOffset, numTotalRows, numTotalColumns)
    }
    
    def init(partitionInformation: Partition, initialValue: Double): Submatrix = {
      Submatrix(GilbertMatrix.init(partitionInformation.numRows, partitionInformation.numColumns, initialValue),
          partitionInformation.rowIndex, partitionInformation.columnIndex, partitionInformation.rowOffset,
          partitionInformation.columnOffset, partitionInformation.numTotalRows, partitionInformation.numTotalColumns)
    }
    
    def eye(partitionInformation: Partition): Submatrix = {
      import partitionInformation._
      Submatrix(GilbertMatrix.eye(numRows, numColumns), rowIndex, columnIndex, rowOffset, columnOffset, numTotalRows,
          numTotalColumns)
    }
    
    def rand(partition: Partition, random: Random = new Random()): Submatrix = {
      import partition._
      Submatrix(GilbertMatrix.rand(numRows, numColumns, random), rowIndex, columnIndex, rowOffset, columnOffset,
          numTotalRows, numTotalColumns)
    }
  
  def outputFormatter(elementDelimiter: String, fieldDelimiter: String) = {
    new Function1[Submatrix, String] {
      def apply(submatrix: Submatrix): String = {
        var result = "";
        for (((row, col), value) <- submatrix.activeIterator) {
          result += ((row+1)+submatrix.rowOffset) + fieldDelimiter + ((col+1)+submatrix.columnOffset) + fieldDelimiter + 
          value + elementDelimiter

        }
        result
      }
    }
  }
  
  implicit def canMapValues = { 
    new CanMapValues[Submatrix, Double, Double, Submatrix]{
      override def map(submatrix: Submatrix, fun: Double => Double) = {
        val gilbert:GilbertMatrix = submatrix.matrix.map(fun)
        import submatrix._
        Submatrix(gilbert, rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
      }
      
      override def mapActive(submatrix: Submatrix, fun: Double => Double) = {
        Submatrix(submatrix.matrix.mapActiveValues(fun), submatrix.rowIndex, submatrix.columnIndex, submatrix.rowOffset,
            submatrix.columnOffset, submatrix.totalRows, submatrix.totalColumns)
      }
    }
  }
  
  implicit def handholdCMV = new CanMapValues.HandHold[Submatrix, Double]
  
  implicit def canZipMapValues:CanZipMapValues[Submatrix, Double, Double, Submatrix] = {
    new CanZipMapValues[Submatrix, Double, Double, Submatrix]{
      override def map(a: Submatrix, b: Submatrix, fn: (Double, Double) => Double) = {
        val mapper = implicitly[CanZipMapValues[GilbertMatrix, Double, Double, GilbertMatrix]]
        val result = mapper.map(a.matrix, b.matrix, fn)
        Submatrix(result, a.rowIndex, a.columnIndex, a.rowOffset, a.columnOffset, a.totalRows, a.totalColumns)
      }
    }
  }
  
  implicit def canIterateValues:CanTraverseValues[Submatrix, Double] = {
    new CanTraverseValues[Submatrix, Double]{
      override def isTraversableAgain(submatrix: Submatrix): Boolean = true
      
      override def traverse(submatrix: Submatrix, fn: ValuesVisitor[Double]) {
        val traversal = implicitly[CanTraverseValues[GilbertMatrix, Double]]
        traversal.traverse(submatrix.matrix, fn)
      }
    }
  }
  
  implicit def canSliceRowSubmatrix: CanSlice2[Submatrix, Int, ::.type, Submatrix] = {
    new CanSlice2[Submatrix, Int, ::.type, Submatrix]{
      override def apply(submatrix: Submatrix, row: Int, ignored: ::.type) = {
        Submatrix(submatrix.matrix(row, ::), submatrix.rowIndex, submatrix.columnIndex, submatrix.rowOffset,
            submatrix.columnOffset, submatrix.totalRows, submatrix.totalColumns)
      }
    }
  }
  
  implicit def canSliceRowsSubmatrix: CanSlice2[Submatrix, Range, ::.type, Submatrix] = {
    new CanSlice2[Submatrix, Range, ::.type, Submatrix]{
      override def apply(submatrix: Submatrix, rows: Range, ignored: ::.type) = {
        Submatrix(submatrix.matrix(rows, ::), submatrix.rowIndex, submatrix.columnIndex, submatrix.rowOffset,
            submatrix.columnOffset, submatrix.totalRows, submatrix.totalColumns)
      }
    }
  }
  
  implicit def canSliceColSubmatrix: CanSlice2[Submatrix, ::.type, Int, Subvector] = {
    new CanSlice2[Submatrix, ::.type, Int, Subvector]{
      override def apply(submatrix: Submatrix, ignored: ::.type, col: Int) ={
        Subvector(submatrix.matrix(::, col), submatrix.rowIndex, submatrix.rowOffset, submatrix.totalRows)
      }
    }
  }
  
  implicit def canSliceColsSubmatrix: CanSlice2[Submatrix, ::.type, Range, Submatrix] = {
      new CanSlice2[Submatrix, ::.type, Range, Submatrix]{
        override def apply(submatrix: Submatrix, ignored: ::.type, cols: Range) = {
          Submatrix(submatrix.matrix(::, cols), submatrix.rowIndex, submatrix.columnIndex, submatrix.rowOffset, 
              submatrix.columnOffset, submatrix.totalRows, submatrix.totalColumns)
        }
      }
  }
  
  implicit def canCollapseRows: CanCollapseAxis[Submatrix, Axis._0.type, BreezeVector[Double], Double, Submatrix] = {
    new CanCollapseAxis[Submatrix, Axis._0.type, BreezeVector[Double], Double, Submatrix]{
      override def apply(submatrix: Submatrix, axis: Axis._0.type)(fn: BreezeVector[Double] => Double) = {
        val collapser = 
          implicitly[CanCollapseAxis[GilbertMatrix, Axis._0.type, BreezeVector[Double], Double, GilbertMatrix]]
        Submatrix(collapser(submatrix.matrix, axis)(fn), 0, submatrix.columnIndex, 0, submatrix.columnOffset,
            1, submatrix.totalColumns)
      }
    }
  }
  
  implicit def canCollapseCols: CanCollapseAxis[Submatrix, Axis._1.type, BreezeVector[Double], Double, Subvector] = {
    new CanCollapseAxis[Submatrix, Axis._1.type, BreezeVector[Double], Double, Subvector]{
      override def apply(submatrix: Submatrix, axis: Axis._1.type)(fn: BreezeVector[Double] => Double) = {
        val collapser = 
          implicitly[CanCollapseAxis[GilbertMatrix, Axis._1.type, BreezeVector[Double], Double, GilbertVector]]
        Subvector(collapser(submatrix.matrix, axis)(fn), submatrix.rowIndex, submatrix.rowOffset, submatrix.totalRows)
      }
    }
  }
  
  implicit def handholdCanMapCols: CanCollapseAxis.HandHold[Submatrix, Axis._1.type, BreezeVector[Double]] = 
    new CanCollapseAxis.HandHold[Submatrix, Axis._1.type, BreezeVector[Double]]()
    
  implicit def handholdCanMapRows: CanCollapseAxis.HandHold[Submatrix, Axis._0.type, BreezeVector[Double]] = 
    new CanCollapseAxis.HandHold[Submatrix, Axis._0.type, BreezeVector[Double]]()

}

