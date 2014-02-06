package org.gilbertlang.runtimeMacros.linalg

import java.io.DataInput
import breeze.linalg.{Vector => BreezeVector}
import java.io.DataOutput
import breeze.linalg.VectorBuilder

object VectorSerialization {
  val sparseVectorId = "sparseVector"
  val denseVectorId = "denseVector"
  
  def read(in: DataInput): BreezeVector[Double] = {
    val id = in.readUTF()
    
    id match {
      case `sparseVectorId` => readSparseVector(in)
      case `denseVectorId` => readDenseVector(in)
    }
  }
  
  def write(vector: BreezeVector[Double], out: DataOutput) {
    vector match {
      case x: Configuration.SparseVector => writeSparseVector(x, out)
      case x: Configuration.DenseVector => writeDenseVector(x, out)
    }
  }
  
  def writeSparseVector(vector: Configuration.SparseVector, out: DataOutput){
    out.writeUTF(sparseVectorId)
    out.writeInt(vector.length)
    out.writeInt(vector.activeSize)
    
    for((index,value) <- vector.activeIterator){
      out.writeInt(index)
      out.writeDouble(value)
    }
  }
  
  def writeDenseVector(vector: Configuration.DenseVector, out: DataOutput){
    out.writeUTF(denseVectorId)
    out.writeInt(vector.length)
    
    for(index <- 0 until vector.length){
      out.writeDouble(vector(index))
    }
  }
  
  def readSparseVector(in: DataInput) = {
    val length = in.readInt()
    val used = in.readInt()
    
    val builder = new VectorBuilder[Double](length, used)
    
    for(_ <- 0 until used){
      val index = in.readInt()
      val value = in.readDouble()
      
      builder.add(index, value)
    }
    
    builder.toSparseVector
  }
  
  def readDenseVector(in: DataInput) = {
    val length = in.readInt()
    val data = new Array[Double](length)
    
    for(index <- 0 until length)
      data(index) = in.readDouble()
    
    new Configuration.DenseVector(data)
  }
}