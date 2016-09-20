package org.gilbertlang.runtimeMacros.linalg.io

import java.io.DataOutput
import java.io.DataInput

trait Serializer[@specialized(Double, Boolean) T] {
  def write(value: T, out: DataOutput)
  def read(in: DataInput): T
}

object Serializer{
   implicit object doubleSerializer extends Serializer[Double]{
     def write(value: Double, out: DataOutput){
       out.writeDouble(value)
     }
     
     def read(in: DataInput): Double ={
       in.readDouble()
     }
   } 
   
   implicit object booleanSerializer extends Serializer[Boolean]{
     def write(value: Boolean, out: DataOutput){
       out.writeBoolean(value)
     }
     
     def read(in: DataInput): Boolean ={
       in.readBoolean()
     }
   }
}