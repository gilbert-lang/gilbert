package org.gilbertlang.runtime.execution.stratosphere

import org.apache.mahout.math.Matrix
import org.apache.mahout.math.Vector
import eu.stratosphere.types.Value
import java.io.DataOutput
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import scala.collection.convert.WrapAsScala
import java.io.IOException
import java.io.DataInput
import eu.stratosphere.core.io.IOReadableWritable
import org.apache.mahout.math.function.DoubleFunction
import org.apache.mahout.math.function.DoubleDoubleFunction
import org.apache.mahout.math.function.VectorFunction

class MatrixValue(var matrix: Matrix = null) extends Matrix with Value with WrapAsScala {

  def write(out: DataOutput) {
    out.writeUTF(matrix.getClass.getName)
    out.writeInt(rowSize())
    out.writeInt(columnSize())

    writeMatrix(out)
  }

  def writeMatrix(out: DataOutput) {
    if (matrix.isInstanceOf[IOReadableWritable]) {
      val iorw = matrix.asInstanceOf[IOReadableWritable]
      iorw.write(out)
    } else {
      var counter = 0
      val buffer = new ByteArrayOutputStream()
      val tempout = new DataOutputStream(buffer)
      for (slice <- this.iterator()) {
        for (element <- slice.vector().nonZeroes()) {
          tempout.writeInt(slice.index())
          tempout.writeInt(element.index())
          tempout.writeDouble(element.get())
          counter += 1
        }
      }

      out.writeInt(counter);
      out.write(buffer.toByteArray())

      try {
        tempout.close()
      } catch {
        case ex: IOException => {
          ex.printStackTrace()
          System.exit(-1)
        }
      }
    }

  }

  def read(in: DataInput) {
    val clazzName = in.readUTF()
    val numRows = in.readInt()
    val numCols = in.readInt()
    val clazz = Class.forName(clazzName)
    val constructor = clazz.getConstructor(classOf[Int], classOf[Int])
    matrix = constructor.newInstance(numRows: java.lang.Integer, numCols: java.lang.Integer).asInstanceOf[MatrixValue]

    readMatrix(in)
  }

  def readMatrix(in: DataInput) {
    if (matrix.isInstanceOf[IOReadableWritable]) {
      val iorw = matrix.asInstanceOf[IOReadableWritable]
      iorw.read(in)
    } else {
      val numElements = in.readInt()

      for (_ <- 0 until numElements) {
        val row = in.readInt()
        val col = in.readInt()
        val value = in.readDouble()
        setQuick(row, col, value)
      }
    }
  }

  def iterator() = matrix.iterator

  def iterateAll() = matrix.iterateAll

  def numSlices = matrix.numSlices()

  def numRows = matrix.numRows()

  def numCols = matrix.numCols()

  /**
   * Return a new vector with cardinality equal to getNumRows() of this matrix which is the matrix product of the
   * recipient and the argument
   *
   * @param v a vector with cardinality equal to getNumCols() of the recipient
   * @return a new vector (typically a DenseVector)
   * @throws CardinalityException if this.getNumRows() != v.size()
   */
  def times(v: Vector) = new VectorValue(matrix.times(v))

  /**
   * Convenience method for producing this.transpose().times(this.times(v)), which can be implemented with only one pass
   * over the matrix, without making the transpose() call (which can be expensive if the matrix is sparse)
   *
   * @param v a vector with cardinality equal to getNumCols() of the recipient
   * @return a new vector (typically a DenseVector) with cardinality equal to that of the argument.
   * @throws CardinalityException if this.getNumCols() != v.size()
   */
  def timesSquared(v: Vector) = new VectorValue(matrix.timesSquared(v))

  /** @return a formatted String suitable for output */
  def asFormatString = matrix.asFormatString();

  /**
   * Assign the value to all elements of the receiver
   *
   * @param value a double value
   * @return the modified receiver
   */
  def assign(value: Double) = {
    matrix.assign(value)
    this
  }

  /**
   * Assign the values to the receiver
   *
   * @param values an Array[Array[Double]] of values
   * @return the modified receiver
   * @throws CardinalityException if the cardinalities differ
   */
  def assign(values: Array[Array[Double]]) = {
    matrix.assign(values)
    this
  }

  /**
   * Assign the other vector values to the receiver
   *
   * @param other a Matrix
   * @return the modified receiver
   * @throws CardinalityException if the cardinalities differ
   */
  def assign(other: Matrix) = {
    matrix.assign(other)
    this
  }

  /**
   * Apply the function to each element of the receiver
   *
   * @param function a DoubleFunction to apply
   * @return the modified receiver
   */
  def assign(function: DoubleFunction) = {
    matrix.assign(function)
    this
  }

  /**
   * Apply the function to each element of the receiver and the corresponding element of the other argument
   *
   * @param other    a Matrix containing the second arguments to the function
   * @param function a DoubleDoubleFunction to apply
   * @return the modified receiver
   * @throws CardinalityException if the cardinalities differ
   */
  def assign(other: Matrix, function: DoubleDoubleFunction) = {
    matrix.assign(other, function)
    this
  }

  /**
   * Assign the other vector values to the column of the receiver
   *
   * @param column the int row to assign
   * @param other  a Vector
   * @return the modified receiver
   * @throws CardinalityException if the cardinalities differ
   */
  def assignColumn(column: Int, other: Vector) = {
    matrix.assignColumn(column, other)
    this
  }

  /**
   * Assign the other vector values to the row of the receiver
   *
   * @param row   the int row to assign
   * @param other a Vector
   * @return the modified receiver
   * @throws CardinalityException if the cardinalities differ
   */
  def assignRow(row: Int, other: Vector) = {
    matrix.assignRow(row, other)
    this
  }

  /**
   * Collects the results of a function applied to each row of a matrix.
   * @param f  The function to be applied to each row.
   * @return  The vector of results.
   */
  def aggregateRows(f: VectorFunction) = new VectorValue(matrix.aggregateRows(f))

  /**
   * Collects the results of a function applied to each column of a matrix.
   * @param f  The function to be applied to each column.
   * @return  The vector of results.
   */
  def aggregateColumns(f: VectorFunction) = new VectorValue(matrix.aggregateColumns(f))

  /**
   * Collects the results of a function applied to each element of a matrix and then
   * aggregated.
   * @param combiner  A function that combines the results of the mapper.
   * @param mapper  A function to apply to each element.
   * @return  The result.
   */
  def aggregate(combiner: DoubleDoubleFunction, mapper: DoubleFunction) = matrix.aggregate(combiner, mapper)

  /**
   * @return The number of rows in the matrix.
   */
  def columnSize = matrix.columnSize()

  /**
   * @return Returns the number of rows in the matrix.
   */
  def rowSize = matrix.rowSize()

  /**
   * Return a copy of the recipient
   *
   * @return a new Matrix
   */
  override def clone() = {
    new MatrixValue(Helper.clone(matrix))
  }

  /**
   * Returns matrix determinator using Laplace theorem
   *
   * @return a matrix determinator
   */
  def determinant = matrix.determinant

  /**
   * Return a new matrix containing the values of the recipient divided by the argument
   *
   * @param x a double value
   * @return a new Matrix
   */
  def divide(x: Double) = new MatrixValue(matrix.divide(x))

  /**
   * Return the value at the given indexes
   *
   * @param row    an int row index
   * @param column an int column index
   * @return the double at the index
   * @throws IndexException if the index is out of bounds
   */
  def get(row: Int, column: Int) = matrix.get(row, column)

  /**
   * Return the value at the given indexes, without checking bounds
   *
   * @param row    an int row index
   * @param column an int column index
   * @return the double at the index
   */
  def getQuick(row: Int, column: Int) = matrix.getQuick(row, column)

  /**
   * Return an empty matrix of the same underlying class as the receiver
   *
   * @return a Matrix
   */
  def like = new MatrixValue(matrix.like())
  /**
   * Returns an empty matrix of the same underlying class as the receiver and of the specified size.
   *
   * @param rows    the int number of rows
   * @param columns the int number of columns
   */
  def like(rows: Int, columns: Int) = new MatrixValue(matrix.like(rows, columns))

  /**
   * Return a new matrix containing the element by element difference of the recipient and the argument
   *
   * @param x a Matrix
   * @return a new Matrix
   * @throws CardinalityException if the cardinalities differ
   */
  def minus(x: Matrix) = new MatrixValue(matrix.minus(x))

  /**
   * Return a new matrix containing the sum of each value of the recipient and the argument
   *
   * @param x a double
   * @return a new Matrix
   */
  def plus(x: Double) = new MatrixValue(matrix.plus(x))

  /**
   * Return a new matrix containing the element by element sum of the recipient and the argument
   *
   * @param x a Matrix
   * @return a new Matrix
   * @throws CardinalityException if the cardinalities differ
   */
  def plus(x: Matrix) = new MatrixValue(matrix.plus(x))

  /**
   * Set the value at the given index
   *
   * @param row    an int row index into the receiver
   * @param column an int column index into the receiver
   * @param value  a double value to set
   * @throws IndexException if the index is out of bounds
   */
  def set(row: Int, column: Int, value: Double) {
    matrix.set(row, column, value)
  }

  def set(row: Int, data: Array[Double]) {
    matrix.set(row, data)
  }

  /**
   * Set the value at the given index, without checking bounds
   *
   * @param row    an int row index into the receiver
   * @param column an int column index into the receiver
   * @param value  a double value to set
   */
  def setQuick(row: Int, column: Int, value: Double) {
    matrix.setQuick(row, column, value)
  }

  /**
   * Return the number of values in the recipient
   *
   * @return an int[2] containing [row, column] count
   */
  def getNumNondefaultElements = matrix.getNumNondefaultElements()

  /**
   * Return a new matrix containing the product of each value of the recipient and the argument
   *
   * @param x a double argument
   * @return a new Matrix
   */
  def times(x: Double) = new MatrixValue(matrix.times(x))
  /**
   * Return a new matrix containing the product of the recipient and the argument
   *
   * @param x a Matrix argument
   * @return a new Matrix
   * @throws CardinalityException if the cardinalities are incompatible
   */
  def times(x: Matrix) = new MatrixValue(matrix.times(x))

  /**
   * Return a new matrix that is the transpose of the receiver
   *
   * @return the transpose
   */
  def transpose() = new MatrixValue(matrix.transpose())

  /**
   * Return the sum of all the elements of the receiver
   *
   * @return a double
   */
  def zSum() = matrix.zSum

  /**
   * Return a map of the current column label bindings of the receiver
   *
   * @return a Map<String, Integer>
   */
  def getColumnLabelBindings() = matrix.getColumnLabelBindings()

  /**
   * Return a map of the current row label bindings of the receiver
   *
   * @return a Map<String, Integer>
   */
  def getRowLabelBindings() = matrix.getRowLabelBindings()

  /**
   * Sets a map of column label bindings in the receiver
   *
   * @param bindings a Map<String, Integer> of label bindings
   */
  def setColumnLabelBindings(bindings: java.util.Map[String, Integer]) {
    matrix.setColumnLabelBindings(bindings)
  }

  /**
   * Sets a map of row label bindings in the receiver
   *
   * @param bindings a Map<String, Integer> of label bindings
   */
  def setRowLabelBindings(bindings: java.util.Map[String, Integer]) {
    matrix.setRowLabelBindings(bindings)
  }

  /**
   * Return the value at the given labels
   *
   * @param rowLabel    a String row label
   * @param columnLabel a String column label
   * @return the double at the index
   *
   * @throws IndexException if the index is out of bounds
   */
  def get(rowLabel: String, columnLabel: String) = matrix.get(rowLabel, columnLabel)

  /**
   * Set the value at the given index
   *
   * @param rowLabel    a String row label
   * @param columnLabel a String column label
   * @param value       a double value to set
   * @throws IndexException if the index is out of bounds
   */
  def set(rowLabel: String, columnLabel: String, value: Double) {
    matrix.set(rowLabel, columnLabel, value)
  }

  /**
   * Set the value at the given index, updating the row and column label bindings
   *
   * @param rowLabel    a String row label
   * @param columnLabel a String column label
   * @param row         an int row index
   * @param column      an int column index
   * @param value       a double value
   */
  def set(rowLabel: String, columnLabel: String, row: Int, column: Int, value: Double) {
    matrix.set(rowLabel, columnLabel, row, column, value)
  }

  /**
   * Sets the row values at the given row label
   *
   * @param rowLabel a String row label
   * @param rowData  a double[] array of row data
   */
  def set(rowLabel: String, rowData: Array[Double]) {
    matrix.set(rowLabel, rowData)
  }

  /**
   * Sets the row values at the given row index and updates the row labels
   *
   * @param rowLabel the String row label
   * @param row      an int the row index
   * @param rowData  a double[] array of row data
   */
  def set(rowLabel: String, row: Int, rowData: Array[Double]) {
    matrix.set(rowLabel, row, rowData)
  }

  /**
   * Return a view into part of a matrix.  Changes to the view will change the
   * original matrix.
   *
   * @param offset an int[2] offset into the receiver
   * @param size   the int[2] size of the desired result
   * @return a matrix that shares storage with part of the original matrix.
   * @throws CardinalityException if the length is greater than the cardinality of the receiver
   * @throws IndexException       if the offset is negative or the offset+length is outside of the receiver
   */
  def viewPart(offset: Array[Int], size: Array[Int]) = new MatrixValue(matrix.viewPart(offset, size))

  /**
   * Return a view into part of a matrix.  Changes to the view will change the
   * original matrix.
   *
   * @param rowOffset           The first row of the view
   * @param rowsRequested       The number of rows in the view
   * @param columnOffset        The first column in the view
   * @param columnsRequested    The number of columns in the view
   * @return a matrix that shares storage with part of the original matrix.
   * @throws CardinalityException if the length is greater than the cardinality of the receiver
   * @throws IndexException       if the offset is negative or the offset+length is outside of the
   *                              receiver
   */
  def viewPart(rowOffset: Int, rowsRequested: Int, columnOffset: Int, columnsRequested: Int) = {
    new MatrixValue(matrix.viewPart(rowOffset, rowsRequested, columnOffset, columnsRequested))
  }

  /**
   * Return a reference to a row.  Changes to the view will change the original matrix.
   * @param row  The index of the row to return.
   * @return A vector that shares storage with the original.
   */
  def viewRow(row: Int) = new VectorValue(matrix.viewRow(row))

  /**
   * Return a reference to a column.  Changes to the view will change the original matrix.
   * @param column  The index of the column to return.
   * @return A vector that shares storage with the original.
   */
  def viewColumn(column: Int) = new VectorValue(matrix.viewColumn(column))

  /**
   * Returns a reference to the diagonal of a matrix. Changes to the view will change
   * the original matrix.
   * @return A vector that shares storage with the original matrix.
   */
  def viewDiagonal() = new VectorValue(matrix.viewDiagonal())

}

object MatrixValue{
  implicit def matrix2MatrixValue(matrix:Matrix) = new MatrixValue(matrix)
}