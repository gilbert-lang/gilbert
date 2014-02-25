package org.gilbertlang.runtimeMacros.linalg.io

import java.io.DataInput

trait DataReader[@specialized(Double, Boolean) T] {
  def read(in: DataInput): T
}

object DataReader{
  implicit object DoubleReader extends DataReader[Double] {
    override def read(in: DataInput) = in.readDouble()
  }
  
  implicit object BooleanReader extends DataReader[Boolean] {
    override def read(in: DataInput) = in.readBoolean()
  }
}