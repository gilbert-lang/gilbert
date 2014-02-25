package org.gilbertlang.runtimeMacros.linalg.io

import java.io.DataOutput

trait DataWriter[@specialized(Double, Boolean) T] {
  def write(value: T, out: DataOutput)
}

object DataWriter{
  implicit object DoubleWriter extends DataWriter[Double]{
    override def write(value: Double, out: DataOutput){
      out.writeDouble(value)
    }
  }
  
  implicit object BooleanWriter extends DataWriter[Boolean]{
    override def write(value: Boolean, out: DataOutput){
      out.writeBoolean(value)
    }
  }
}