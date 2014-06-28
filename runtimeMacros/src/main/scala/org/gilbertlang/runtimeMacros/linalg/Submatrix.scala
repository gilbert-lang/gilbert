package org.gilbertlang.runtimeMacros.linalg

import _root_.breeze.linalg.support.CanSlice2
import _root_.breeze.stats.distributions.{Uniform, Rand}
import eu.stratosphere.types.Value
import java.io.{DataInput, DataOutput}

import org.gilbertlang.runtimeMacros.linalg.breeze.BreezeDoubleMatrixFactory
import org.gilbertlang.runtimeMacros.linalg.operators.SubmatrixImplicits
import org.gilbertlang.runtimeMacros.linalg.serialization.MatrixSerialization

case class Submatrix(var matrixValue: DoubleMatrixValue, var rowIndex: Int, var columnIndex: Int,
                     var rowOffset: Int, var columnOffset: Int, var totalRows: Int,
                     var totalColumns: Int) extends Value {

  implicit def boolean2BooleanSubmatrix(matrix: BooleanMatrix): BooleanSubmatrix = {
    BooleanSubmatrix(matrix, rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
  }

  def matrix = matrixValue.matrix

  def rows = matrix.rows
  def cols = matrix.cols

  def rowRange = rowOffset until (rowOffset + rows)
  def colRange = columnOffset until (columnOffset + cols)

  def apply(i: Int, j: Int): Double = matrix(i-rowOffset, j-columnOffset)
  def apply[Slice1, Slice2, Result](slice1: Slice1, slice2: Slice2)(implicit canSlice: CanSlice2[Submatrix, Slice1,
    Slice2, Result]): Result = {
    canSlice(this, slice1, slice2)
  }

  def mapActiveValues(func: Double => Double): Submatrix = {
    Submatrix(matrix.mapActiveValues(func), rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
  }

  def copy = new Submatrix(this.matrix.copy, rowIndex, columnIndex,
      rowOffset, columnOffset, totalRows, totalColumns)

  def update(i: Int, j: Int, value: Double) = matrix.update((i-rowOffset, j-columnOffset), value)

//  override def repr = this

//  override def iterator = matrix.iterator map { case ((row, col), value ) => ((row + rowOffset, col + columnOffset),
//    value)}
//  override def keysIterator = matrix.keysIterator map { case (row, col) => (row+ rowOffset, col + columnOffset)}
  def activeIterator = matrix.activeIterator map { case ((row, col), value) => ((row+ rowOffset, col + columnOffset),
  value)}
  def iterator: Iterator[((Int, Int), Double)] = matrix.iterator map { case ((row, col), value) => ((row + rowOffset,
   col + columnOffset), value)}
//  def activeKeysIterator = matrix.activeKeysIterator map { case (row, col) => (row+ rowOffset, col+ columnOffset)}
//  def activeValuesIterator = matrix.activeValuesIterator
//
//  def activeSize = matrix.activeSize

  def this() = this(null, -1, -1, -1, -1, -1, -1)

  override def toString(): String = {
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

  def +(op: Submatrix): Submatrix = { Submatrix(matrix + op.matrix, rowIndex, columnIndex, rowOffset,
    columnOffset, totalRows, totalColumns) }
  def -(op: Submatrix): Submatrix = { Submatrix(matrix - op.matrix, rowIndex, columnIndex, rowOffset, columnOffset,
    totalRows, totalColumns)}
  def /(op: Submatrix): Submatrix = { Submatrix(matrix / op.matrix, rowIndex, columnIndex, rowOffset, columnOffset,
    totalRows, totalColumns)}
  def :/(op: Submatrix): Submatrix = { Submatrix(matrix / op.matrix, rowIndex, columnIndex, rowOffset, columnOffset,
    totalRows, totalColumns)}
  def *(op: Submatrix): Submatrix = { Submatrix(matrix * op.matrix, rowIndex, op.columnIndex, rowOffset,
    op.columnOffset,totalRows, op.totalColumns)}
  def :^(op: Submatrix): Submatrix = { Submatrix(matrix :^ op.matrix, rowIndex, columnIndex, rowOffset, columnOffset,
    totalRows, totalColumns)
  }

  def :*(op: Submatrix): Submatrix = { Submatrix(matrix:* op.matrix, rowIndex, columnIndex, rowOffset, columnOffset,
    totalRows, totalColumns)}

  def +(sc: Double): Submatrix = { Submatrix(matrix + sc, rowIndex, columnIndex, rowOffset, columnOffset, totalRows,
    totalColumns)}
  def -(sc: Double): Submatrix = { Submatrix(matrix - sc, rowIndex, columnIndex, rowOffset, columnOffset, totalRows,
    totalColumns)}
  def /(sc: Double): Submatrix = { Submatrix(matrix / sc, rowIndex, columnIndex, rowOffset, columnOffset, totalRows,
    totalColumns)}
  def *(sc: Double): Submatrix = { Submatrix(matrix * sc, rowIndex, columnIndex, rowOffset, columnOffset, totalRows,
    totalColumns)}
  def :^(sc: Double): Submatrix = { Submatrix(matrix :^ sc, rowIndex, columnIndex, rowOffset, columnOffset,
    totalRows, totalColumns)}

  def :>(m: Submatrix) : BooleanSubmatrix = { matrix :> m.matrix }
  def :>=(m: Submatrix): BooleanSubmatrix = { matrix :>= m.matrix }
  def :<(m: Submatrix): BooleanSubmatrix = { matrix :< m.matrix }
  def :<=(m: Submatrix): BooleanSubmatrix = { matrix :<= m.matrix }
  def :==(m: Submatrix): BooleanSubmatrix = { matrix :== m.matrix }
  def :!=(m: Submatrix): BooleanSubmatrix = { matrix :!= m.matrix }

  def :>(sc: Double): BooleanSubmatrix = { BooleanSubmatrix(matrix :> sc, rowIndex, columnIndex, rowOffset,
    columnOffset,
    totalRows, totalColumns)}
  def :>=(sc: Double): BooleanSubmatrix = { BooleanSubmatrix(matrix :>= sc, rowIndex, columnIndex, rowOffset,
    columnOffset,
    totalRows, totalColumns)}
  def :<(sc: Double): BooleanSubmatrix = { BooleanSubmatrix(matrix :< sc, rowIndex, columnIndex, rowOffset,
    columnOffset,
    totalRows, totalColumns)}
  def :<=(sc: Double): BooleanSubmatrix = { BooleanSubmatrix(matrix :<= sc, rowIndex, columnIndex, rowOffset,
    columnOffset,
    totalRows, totalColumns)}
  def :==(sc: Double): BooleanSubmatrix = { BooleanSubmatrix(matrix :== sc, rowIndex, columnIndex, rowOffset,
    columnOffset,
    totalRows, totalColumns)}
  def :!=(sc: Double): BooleanSubmatrix = { BooleanSubmatrix(matrix :!= sc, rowIndex, columnIndex, rowOffset,
    columnOffset,
    totalRows, totalColumns)}


  def t: Submatrix = {
    Submatrix(matrix.t, columnIndex, rowIndex, columnOffset, rowOffset, totalColumns, totalRows)
  }

  def write(out: DataOutput): Unit = {
    matrixValue.write(out)
    out.writeInt(rowIndex)
    out.writeInt(columnIndex)
    out.writeInt(rowOffset)
    out.writeInt(columnOffset)
    out.writeInt(totalRows)
    out.writeInt(totalColumns)
  }

  def read(in: DataInput): Unit = {
    matrixValue = new DoubleMatrixValue()
    matrixValue.read(in)
    rowIndex = in.readInt()
    columnIndex = in.readInt()
    rowOffset = in.readInt()
    columnOffset = in.readInt()
    totalRows = in.readInt()
    totalColumns = in.readInt()
  }
}

object Submatrix extends SubmatrixImplicits {
  var matrixFactory: DoubleMatrixFactory = BreezeDoubleMatrixFactory

  def apply(partitionInformation: Partition, entries: Traversable[(Int,Int,Double)]): Submatrix = {
    import partitionInformation._
    val adjustedEntries = entries map { case (row, col, value) => (row - rowOffset, col - columnOffset, value)}

    val dense = adjustedEntries.size.toDouble/(numRows*numColumns) > Configuration.DENSITYTHRESHOLD

    val matrix = matrixFactory.create(numRows, numColumns, adjustedEntries, dense)
    Submatrix(matrix, rowIndex, columnIndex, rowOffset,columnOffset, numTotalRows, numTotalColumns)
  }


  def apply(partitionInformation: Partition, numNonZeroElements: Int = 0): Submatrix = {
      import partitionInformation._

      val dense = numNonZeroElements.toDouble/(numRows*numColumns) > Configuration.DENSITYTHRESHOLD

      Submatrix(matrixFactory.create(numRows, numColumns, dense), rowIndex, columnIndex, rowOffset,
          columnOffset, numTotalRows, numTotalColumns)
    }
    
    def init(partitionInformation: Partition, initialValue: Double): Submatrix = {
      import partitionInformation._
      Submatrix(matrixFactory.init(numRows, numColumns, initialValue, dense = true), rowIndex, columnIndex,
        rowOffset, columnOffset, numTotalRows, numTotalColumns)
    }
    
    def eye(partitionInformation: Partition): Submatrix = {
      import partitionInformation._

      val matrix = containsDiagonal(partitionInformation) match {
        case None => matrixFactory.create(numRows, numColumns, dense = false)
        case Some(startIdx) =>
          val (startRow, startColumn) = (startIdx - rowOffset, startIdx - columnOffset)
          val dense = math.min(numRows-startRow, numColumns-startColumn).toDouble/(numRows*numColumns) >
            Configuration.DENSITYTHRESHOLD
          matrixFactory.eye(numRows, numColumns, startRow, startColumn, dense)
      }

      Submatrix(matrix, rowIndex, columnIndex, rowOffset, columnOffset, numTotalRows,
          numTotalColumns)
    }

    def containsDiagonal(partitionInformation: Partition): Option[Int] = {
      import partitionInformation._

      if(columnOffset + numColumns <= rowOffset || rowOffset+numRows <= columnOffset){
        None
      }else{
        if(columnOffset <= rowOffset){
          Some(rowOffset)
        }else{
          Some(columnOffset)
        }
      }
    }
    
    def rand(partition: Partition, random: Rand[Double] = new Uniform(0,1)): Submatrix = {
      import partition._
      Submatrix(matrixFactory.rand(numRows, numColumns, random), rowIndex, columnIndex, rowOffset, columnOffset,
          numTotalRows, numTotalColumns)
    }
  
  def outputFormatter(elementDelimiter: String, fieldDelimiter: String) = {
    new (Submatrix => String) {
      def apply(submatrix: Submatrix): String = {
        var result = ""
        for (((row, col), value) <- submatrix.activeIterator) {
          // + 1 due to matlab indexing conventions which start at 1
          result += (row+1) + fieldDelimiter + (col+1) + fieldDelimiter +
          value + elementDelimiter

        }
        result
      }
    }
  }
}

