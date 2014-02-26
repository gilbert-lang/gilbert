package org.gilbertlang.runtimeMacros.linalg

import java.io.DataInput
import breeze.linalg.{Vector => BreezeVector, DenseVector => BreezeDenseVector, SparseVector => BreezeSparseVector,
  BitVector => BreezeBitVector}
import java.io.DataOutput
import breeze.linalg.VectorBuilder
import org.gilbertlang.runtimeMacros.linalg.io.Serializer
import breeze.storage.DefaultArrayValue
import scala.reflect.ClassTag
import breeze.math.Semiring
import breeze.linalg.BitVector

object VectorSerialization {
  val sparseVectorId = "sparseVector"
  val denseVectorId = "denseVector"
  val bitVectorId = "bitVector"
  
  def read[@specialized(Double, Boolean) T: Serializer: ClassTag: Semiring: DefaultArrayValue](in: DataInput): BreezeVector[T] = {
    val id = in.readUTF()
    
    id match {
      case `sparseVectorId` => readSparseVector(in)
      case `denseVectorId` => readDenseVector(in)
    }
  }
  
  def write[@specialized(Double, Boolean) T: Serializer](vector: BreezeVector[T], out: DataOutput) {
    vector match {
      case x: BreezeSparseVector[T] => 
        out.writeUTF(sparseVectorId)
        writeSparseVector(x, out)
      case x: BreezeDenseVector[T] => 
        out.writeUTF(denseVectorId)
        writeDenseVector(x, out)
    }
  }
  
  def writeSparseVector[@specialized(Double, Boolean) T: Serializer](vector: BreezeSparseVector[T], out: DataOutput){
    out.writeInt(vector.length)
    out.writeInt(vector.activeSize)
    val writer = implicitly[Serializer[T]]
    
    for((index,value) <- vector.activeIterator){
      out.writeInt(index)
      writer.write(value, out)
    }
  }
  
  def writeDenseVector[@specialized(Double, Boolean) T: Serializer](vector: BreezeDenseVector[T], out: DataOutput){
    out.writeInt(vector.length)
    val writer = implicitly[Serializer[T]]
    
    for(index <- 0 until vector.length){
      writer.write(vector(index),out)
    }
  }
  
  def writeBitVector(vector: BreezeBitVector, out: DataOutput){
    out.writeInt(vector.size)
    out.writeInt(vector.activeSize)
    for(index <- vector.activeKeysIterator){
      out.writeInt(index)
    }
  }
  
  def readSparseVector[@specialized(Double, Boolean) T: Serializer: ClassTag: Semiring: DefaultArrayValue](in: DataInput) = {
    val length = in.readInt()
    val used = in.readInt()
    val reader = implicitly[Serializer[T]]
    
    val builder = new VectorBuilder[T](length, used)
    
    for(_ <- 0 until used){
      val index = in.readInt()
      val value = reader.read(in)
      
      builder.add(index, value)
    }
    
    builder.toSparseVector
  }
  
  def readDenseVector[@specialized(Double, Boolean) T: Serializer: ClassTag](in: DataInput) = {
    val length = in.readInt()
    val reader = implicitly[Serializer[T]]
    val data = new Array[T](length)
    
    for(index <- 0 until length)
      data(index) = reader.read(in)
    
    new BreezeDenseVector[T](data)
  }
  
  def readBitVector(in:DataInput) = {
    val length = in.readInt()
    val used = in.readInt()
    val bitset = new java.util.BitSet(length)
    
    for(_ <- 0 until used){
      val index = in.readInt()
      bitset.set(index)
    }
    
    new BitVector(bitset, length)
  }
}