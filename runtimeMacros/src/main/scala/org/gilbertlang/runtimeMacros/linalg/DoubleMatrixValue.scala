package org.gilbertlang.runtimeMacros.linalg

import java.io.{DataInput, DataOutput}

import eu.stratosphere.types.Value
import org.gilbertlang.runtimeMacros.linalg.serialization.MatrixSerialization

class DoubleMatrixValue(var matrix: DoubleMatrix) extends Value {
  def this() = this(null)

  def write(out: DataOutput): Unit = {
    MatrixSerialization.writeDoubleMatrix(matrix, out)
  }

  def read(in: DataInput): Unit = {
    matrix = MatrixSerialization.readDoubleMatrix(in)
  }
}

object DoubleMatrixValue{
  implicit def value2Matrix(value: DoubleMatrixValue): DoubleMatrix = {
    value.matrix
  }

  implicit def matrix2Value(matrix: DoubleMatrix): DoubleMatrixValue = {
    new DoubleMatrixValue(matrix)
  }
}
