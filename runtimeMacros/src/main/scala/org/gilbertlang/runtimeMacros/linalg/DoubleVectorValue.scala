package org.gilbertlang.runtimeMacros.linalg

import java.io._

import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.Value
import org.gilbertlang.runtimeMacros.linalg.serialization.VectorSerialization

class DoubleVectorValue(var vector: DoubleVector) extends Value {

  def this() = this(null)

  override def write(out: DataOutputView): Unit = {
    VectorSerialization.writeDouble(vector, out)
  }

  override def read(in: DataInputView): Unit = {
    vector = VectorSerialization.readDouble(in)
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    vector = VectorSerialization.readDouble(in)
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    VectorSerialization.writeDouble(vector, out)
  }
}

object DoubleVectorValue{
  implicit def vector2Value(vector: DoubleVector): DoubleVectorValue = {
    new DoubleVectorValue(vector)
  }

  implicit def value2Vector(value: DoubleVectorValue): DoubleVector = {
    value.vector
  }

}
