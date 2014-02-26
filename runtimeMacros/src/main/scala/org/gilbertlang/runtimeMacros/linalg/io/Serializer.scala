package org.gilbertlang.runtimeMacros.linalg.io

import java.io.DataOutput
import java.io.DataInput
import scala.reflect.ClassTag
import breeze.math.Semiring
import breeze.storage.DefaultArrayValue

trait Serializer[@specialized(Double, Boolean) T] {
  def write(value: T, out: DataOutput)
  def read(in: DataInput): T
  
  def classTag: ClassTag[T]
  def semiring: Semiring[T]
  def defaultArrayValue: DefaultArrayValue[T]
}

object Serializer extends GenericSerializer{
  implicit val doubleSerializer:Serializer[Double] = new Serializer[Double]{
    def write(value: Double, out: DataOutput){
      out.writeDouble(value)
    }
    
    def read(in: DataInput): Double = {
      in.readDouble()
    }
    
    val classTag = ClassTag.Double
    val semiring = Semiring.semiringD
    val defaultArrayValue = DefaultArrayValue.DoubleDefaultArrayValue
  }
  
  implicit val booleanSerializer:Serializer[Boolean] = new Serializer[Boolean] {
    def write(value: Boolean, out: DataOutput){
      out.writeBoolean(value)
    }
    
    def read(in: DataInput): Boolean = {
      in.readBoolean()
    }
    
    val classTag = ClassTag.Boolean
    val semiring = Semiring.fieldB
    val defaultArrayValue = DefaultArrayValue.BooleanDefaultArrayValue
  }
}

trait GenericSerializer{
   implicit def genericSerializer[T] = new Serializer[T]{
    def write(value: T, out: DataOutput) {
      throw new NotImplementedError("Generic serializer has to be specialized for type: " + value.getClass)
    }
    
    def read(in: DataInput): T = {
      throw new NotImplementedError("Generic serializer has to be specialized.")
    }
    
    def classTag: ClassTag[T] = {
      throw new NotImplementedError("Generic serializer has to be specialized.")
    }
    
    def semiring: Semiring[T] = {
      throw new NotImplementedError("Generic serializer has to be specialized.")
    }
    
    def defaultArrayValue: DefaultArrayValue[T] = {
      throw new NotImplementedError("Generic serializer has to be specialized.")
    }
  }
}