package org.gilbertlang.runtimeMacros.linalg

import java.io._

import eu.stratosphere.types.Value
import org.gilbertlang.runtimeMacros.linalg.serialization.VectorSerialization

class DoubleVectorValue(var vector: DoubleVector) extends Value {

  def this() = this(null)

  def write(out: DataOutput): Unit = {
    VectorSerialization.writeDouble(vector, out)
  }

  def read(in: DataInput): Unit = {
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
