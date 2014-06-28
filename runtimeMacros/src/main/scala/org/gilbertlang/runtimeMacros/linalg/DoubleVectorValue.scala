package org.gilbertlang.runtimeMacros.linalg

import java.io.{DataInput, DataOutput}

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
}

object DoubleVectorValue{
  implicit def vector2Value(vector: DoubleVector): DoubleVectorValue = {
    new DoubleVectorValue(vector)
  }

  implicit def value2Vector(value: DoubleVectorValue): DoubleVector = {
    value.vector
  }

}
