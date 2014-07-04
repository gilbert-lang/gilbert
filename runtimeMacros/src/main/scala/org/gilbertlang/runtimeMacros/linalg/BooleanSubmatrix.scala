package org.gilbertlang.runtimeMacros.linalg

import eu.stratosphere.types.Value
import java.io.{DataInput, DataOutput}

import org.gilbertlang.runtimeMacros.linalg.serialization.MatrixSerialization

case class BooleanSubmatrix(var matrix: BooleanMatrix, var rowIndex: Int, var columnIndex: Int, var rowOffset: Int,
  var columnOffset: Int, var totalRows: Int, var totalColumns: Int) extends Value{

  def rows = matrix.rows
  def cols = matrix.cols

  def rowRange = rowOffset until (rowOffset + rows)
  def colRange = columnOffset until (columnOffset + cols)

  def apply(i: Int, j: Int): Boolean = matrix(i-rowOffset, j-columnOffset)

  def copy = new BooleanSubmatrix(this.matrix.copy, rowIndex, columnIndex,
      rowOffset, columnOffset, totalRows, totalColumns)

  def update(coord: (Int, Int), value: Boolean) = matrix.update((coord._1-rowOffset, coord._2-columnOffset), value)


  def activeIterator = matrix.activeIterator map { case ((row, col), value) => ((row+ rowOffset, col + columnOffset),
    value)}

  def this() = this(null, -1, -1, -1, -1, -1, -1)

  override def toString(): String = {
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

  override def write(out: DataOutput){
    MatrixSerialization.writeBooleanMatrix(matrix, out)
    out.writeInt(rowIndex)
    out.writeInt(columnIndex)
    out.writeInt(rowOffset)
    out.writeInt(columnOffset)
    out.writeInt(totalRows)
    out.writeInt(totalColumns)
  }

  override def read(in: DataInput){
    matrix = MatrixSerialization.readBooleanMatrix(in)
    rowIndex = in.readInt()
    columnIndex = in.readInt()
    rowOffset = in.readInt()
    columnOffset = in.readInt()
    totalRows = in.readInt()
    totalColumns = in.readInt()
  }

  def &(operand: Boolean): BooleanSubmatrix = {
    BooleanSubmatrix(matrix & operand, rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
  }

  def &(operand: BooleanSubmatrix): BooleanSubmatrix = {
    BooleanSubmatrix(matrix & operand.matrix, rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
  }

  def :&(operand: Boolean): BooleanSubmatrix = {
    BooleanSubmatrix(matrix & operand, rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
  }

  def :&(operand: BooleanSubmatrix): BooleanSubmatrix = {
    BooleanSubmatrix(matrix & operand.matrix, rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
  }

  def :|(operand: Boolean): BooleanSubmatrix = {
    BooleanSubmatrix(matrix :| operand, rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
  }

  def :|(operand: BooleanSubmatrix): BooleanSubmatrix = {
    BooleanSubmatrix(matrix :| operand.matrix, rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
  }

  def t: BooleanSubmatrix = {
    BooleanSubmatrix(matrix.t, columnIndex, rowIndex, columnOffset, rowOffset, totalColumns, totalRows)
  }
}

object BooleanSubmatrix {


    def apply(partitionInformation: Partition, entries: Seq[(Int,Int,Boolean)])(implicit factory:
    BooleanMatrixFactory, configuration: RuntimeConfiguration): BooleanSubmatrix = {
      import partitionInformation._
      val adjustedEntries = entries map { case (row, col, value) => (row - rowOffset, col - columnOffset, value)}
      val dense = adjustedEntries.size.toDouble/(numRows*numColumns) > configuration.densityThreshold
      val m = factory.create(numRows, numColumns, adjustedEntries, dense)
      BooleanSubmatrix(m, rowIndex, columnIndex, rowOffset,columnOffset, numTotalRows, numTotalColumns)
    }
  
    def apply(partitionInformation: Partition, numNonZeroElements: Int = 0)(implicit factory: BooleanMatrixFactory,
                                                                            configuration: RuntimeConfiguration):
    BooleanSubmatrix = {
      import partitionInformation._
      val dense = numNonZeroElements.toDouble/(numRows*numColumns) > configuration.densityThreshold
      val m = factory.create(numRows, numColumns, dense)
      BooleanSubmatrix(m, rowIndex, columnIndex, rowOffset,
          columnOffset, numTotalRows, numTotalColumns)
    }
    
    def init(partitionInformation: Partition, initialValue: Boolean)(implicit factory: BooleanMatrixFactory):
    BooleanSubmatrix = {
      import partitionInformation._
      val m = factory.init(numRows, numColumns, initialValue, dense=true)
      BooleanSubmatrix(m, rowIndex, columnIndex, rowOffset, columnOffset, numTotalRows, numTotalColumns)
    }

  def eye(partitionInformation: Partition)(implicit factory: BooleanMatrixFactory,
                                           configuration: RuntimeConfiguration): BooleanSubmatrix = {
    import partitionInformation._

    val matrix = containsDiagonal(partitionInformation) match {
      case None => factory.create(numRows, numColumns, dense = false)
      case Some(startIdx) =>
        val (startRow, startColumn) = (startIdx - rowOffset, startIdx - columnOffset)
        val dense = math.min(numRows-startRow, numColumns-startColumn).toDouble/(numRows*numColumns) >
          configuration.densityThreshold
        factory.eye(numRows, numColumns, startRow, startColumn, dense)
    }

    BooleanSubmatrix(matrix, rowIndex, columnIndex, rowOffset, columnOffset, numTotalRows,
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
  
  def outputFormatter(elementDelimiter: String, fieldDelimiter: String) = {
    new (BooleanSubmatrix => String) {
      def apply(submatrix: BooleanSubmatrix): String = {
        var result = ""
        for (((row, col), value) <- submatrix.activeIterator) {
          result += (row+1) + fieldDelimiter + (col+1) + fieldDelimiter +
          value + elementDelimiter

        }
        result
      }
    }
  }
}

