package org.gilbertlang.runtimeMacros.linalg

import _root_.breeze.linalg.support.ScalarOf
import _root_.breeze.linalg.support.CanSlice2
import _root_.breeze.stats.distributions.{Rand, Uniform}

import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.Value
import org.gilbertlang.runtimeMacros.linalg.operators.SubmatrixImplicits

case class Submatrix(var matrixValue: DoubleMatrixValue, var rowIndex: Int, var columnIndex: Int,
                     var rowOffset: Int, var columnOffset: Int, var totalRows: Int,
                     var totalColumns: Int) extends Value {

  implicit def boolean2BooleanSubmatrix(matrix: BooleanMatrix): BooleanSubmatrix = {
    BooleanSubmatrix(matrix, rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
  }

  def matrix = matrixValue.matrix

  def rows = matrix.rows
  def cols = matrix.cols

  def activeSize: Int = matrix.activeSize

  def rowRange = rowOffset until (rowOffset + rows)
  def colRange = columnOffset until (columnOffset + cols)

  def apply(i: Int, j: Int): Double = {
    matrix(i - rowOffset, j - columnOffset)
  }
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

  def activeIterator = matrix.activeIterator map { case ((row, col), value) => ((row+ rowOffset, col + columnOffset),
  value)}
  def iterator: Iterator[((Int, Int), Double)] = matrix.iterator map { case ((row, col), value) => ((row + rowOffset,
   col + columnOffset), value)}

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

  override def write(out: DataOutputView): Unit = {
    matrixValue.write(out)
    out.writeInt(rowIndex)
    out.writeInt(columnIndex)
    out.writeInt(rowOffset)
    out.writeInt(columnOffset)
    out.writeInt(totalRows)
    out.writeInt(totalColumns)
  }

  override def read(in: DataInputView): Unit = {
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

  implicit val scalarOf = ScalarOf.dummy[Submatrix, Double]

  def apply(partitionInformation: Partition, entries: Traversable[(Int,Int,
    Double)])(implicit factory: DoubleMatrixFactory, configuration: RuntimeConfiguration):
  Submatrix = {
    import partitionInformation._
    val adjustedEntries = entries map { case (row, col, value) => (row - rowOffset, col - columnOffset, value)}

    val dense = adjustedEntries.size.toDouble/(numRows*numColumns) > configuration.densityThreshold

    val matrix = factory.create(numRows, numColumns, adjustedEntries, dense)
    Submatrix(matrix, rowIndex, columnIndex, rowOffset,columnOffset, numTotalRows, numTotalColumns)
  }


  def apply(partitionInformation: Partition, numNonZeroElements: Int = 0)(implicit factory: DoubleMatrixFactory,
                                                                          configuration: RuntimeConfiguration):
  Submatrix = {
      import partitionInformation._

      val dense = numNonZeroElements.toDouble/(numRows*numColumns) > configuration.densityThreshold

      Submatrix(factory.create(numRows, numColumns, dense), rowIndex, columnIndex, rowOffset,
          columnOffset, numTotalRows, numTotalColumns)
    }

    def init(partitionInformation: Partition, initialValue: Double)(implicit factory: DoubleMatrixFactory): Submatrix
    = {
      import partitionInformation._
      Submatrix(factory.init(numRows, numColumns, initialValue, dense = true), rowIndex, columnIndex,
        rowOffset, columnOffset, numTotalRows, numTotalColumns)
    }

    def eye(partitionInformation: Partition)(implicit factory: DoubleMatrixFactory,
                                             configuration: RuntimeConfiguration): Submatrix = {
      import partitionInformation._

      val matrix = containsDiagonal(partitionInformation) match {
        case None => factory.create(numRows, numColumns, dense = false)
        case Some(startIdx) =>
          val (startRow, startColumn) = (startIdx - rowOffset, startIdx - columnOffset)
          val dense = math.min(numRows-startRow, numColumns-startColumn).toDouble/(numRows*numColumns) >
            configuration.densityThreshold
          factory.eye(numRows, numColumns, startRow, startColumn, dense)
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

    def rand(partition: Partition, random: Rand[Double] = new Uniform(0,1))(implicit factory: DoubleMatrixFactory):
    Submatrix = {
      import partition._
      Submatrix(factory.rand(numRows, numColumns, random), rowIndex, columnIndex, rowOffset,
        columnOffset, numTotalRows, numTotalColumns)
    }

    def sprand(partition: Partition, random: Rand[Double], level: Double)(implicit factory: DoubleMatrixFactory):
    Submatrix = {
      import partition._
      Submatrix(factory.sprand(numRows, numColumns, random, level), rowIndex, columnIndex, rowOffset, columnOffset,
        numTotalRows, numTotalColumns)
    }

    def adaptiveRand(partition: Partition, rand: Rand[Double], level: Double,
                     densityThreshold: Double)(implicit factory: DoubleMatrixFactory):
    Submatrix = {
      import partition._
      Submatrix(factory.adaptiveRand(numRows, numColumns, rand, level, densityThreshold), rowIndex,
        columnIndex, rowOffset, columnOffset, numTotalRows, numTotalColumns)
    }
  
  def outputFormatter(elementDelimiter: String, fieldDelimiter: String, verbose: Boolean) = {
    new (Submatrix => String) {
      def apply(submatrix: Submatrix): String = {
        var result = ""
        if(verbose){
          for (((row, col), value) <- submatrix.activeIterator) {
            // + 1 due to matlab indexing conventions which start at 1
            result += (row+1) + fieldDelimiter + (col+1) + fieldDelimiter +
              value + elementDelimiter

          }
        }else{
          import submatrix.{rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns}
          val index = s"Index: ($rowIndex, $columnIndex)"
          val offset = s"Offset: ($rowOffset, $columnOffset)"
          val totalSize = s"Total size: ($totalRows, $totalColumns)"
          val nonZeros = submatrix.activeSize

          result = s"Submatrix[$index $offset $totalSize] #NonZeroes:$nonZeros" + elementDelimiter
        }

        result
      }
    }
  }
}

