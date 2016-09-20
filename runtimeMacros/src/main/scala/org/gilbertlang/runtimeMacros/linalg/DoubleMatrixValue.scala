package org.gilbertlang.runtimeMacros.linalg


import java.io._

import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.Value
import org.gilbertlang.runtimeMacros.linalg.serialization.MatrixSerialization

class DoubleMatrixValue(var matrix: DoubleMatrix) extends Value {
  def this() = this(null)

  override def write(out: DataOutputView): Unit = {
    MatrixSerialization.writeDoubleMatrix(matrix, out)
  }

  override def read(in: DataInputView): Unit = {
    matrix = MatrixSerialization.readDoubleMatrix(in)
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    matrix = MatrixSerialization.readDoubleMatrix(in)
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    MatrixSerialization.writeDoubleMatrix(matrix, out)
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
