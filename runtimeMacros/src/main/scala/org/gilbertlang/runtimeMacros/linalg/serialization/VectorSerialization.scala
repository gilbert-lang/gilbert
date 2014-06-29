package org.gilbertlang.runtimeMacros.linalg.serialization

import java.io.{DataInput, DataOutput}


import breeze.linalg.{VectorBuilder, SparseVector, DenseVector}
import org.gilbertlang.runtimeMacros.linalg.breeze._
import org.gilbertlang.runtimeMacros.linalg.mahout.MahoutDoubleVector
import org.gilbertlang.runtimeMacros.linalg.{ DoubleVector}
import org.apache.mahout.math.{DenseVector => MahoutDenseVector, RandomAccessSparseVector => MahoutSparseVector}
import collection.JavaConversions._


object VectorSerialization {
  val breezeSparseVectorId = "breezeSparseVector"
  val breezeDenseVectorId = "breezeDenseVector"
  val breezeBitvectorId = "breezeBitvector"
  val mahoutSparseVectorId = "mahoutSparseVector"
  val mahoutDenseVectorId = "mahoutDenseVector"
  val mahoutBooleanSparseVector = "mahoutBooleanSparseVector"
  val mahoutBooleanDenseVector = "mahoutBooleanDenseVector"

  def readDouble(in: DataInput): DoubleVector = {
    val tpeId = in.readUTF()

    tpeId match {
      case `breezeDenseVectorId` => readBreezeDenseVector(in)
      case `breezeSparseVectorId` => readBreezeSparseVector(in)
      case `mahoutDenseVectorId` => MahoutDoubleVector(readMahoutDenseVector(in))
      case `mahoutSparseVectorId` => MahoutDoubleVector(readMahoutSparseVector(in))
      case _ =>
        throw new IllegalArgumentException("Vector serialization does not support vectors with type id " + tpeId)
    }
  }

  def writeDouble(vector: DoubleVector, out: DataOutput) = {
    vector match {
      case v: BreezeDoubleVector =>
        writeBreezeDoubleVector(v, out)
      case v: MahoutDoubleVector =>
        writeMahoutDoubleVector(v, out)
      case _ =>
        throw new IllegalArgumentException("Vector serialization cannot serialize vector of type " + vector.getClass)
    }
  }

  def writeBreezeDoubleVector(vector: BreezeDoubleVector, out: DataOutput) = {
    vector.vector match {
      case v:DenseVector[Double] => writeBreezeDenseVector(v, out)
      case v: SparseVector[Double] => writeBreezeSparseVector(v, out)
      case _ => throw new IllegalArgumentException("Vector serialization cannot serialize vector of type " + vector
        .getClass)
    }
  }

  def writeMahoutDoubleVector(vector: MahoutDoubleVector, out: DataOutput) = {
    vector.vector match {
      case v: MahoutDenseVector =>
        out.writeUTF(this.mahoutDenseVectorId)
        writeMahoutDenseVector(v, out)
      case v: MahoutSparseVector =>
        out.writeUTF(this.mahoutSparseVectorId)
        writeMahoutSparseVector(v, out)
      case _ => throw new IllegalArgumentException("Vector serialization cannot serialize vector of type " + vector
        .vector.getClass)
    }
  }

  def writeMahoutDenseVector(vector: MahoutDenseVector, out: DataOutput){
    out.writeInt(vector.size)

    for(idx <- 0 until vector.size){
      out.writeDouble(vector.getQuick(idx))
    }
  }

  def writeMahoutSparseVector(vector: MahoutSparseVector, out: DataOutput){
    out.writeInt(vector.size)
    out.writeInt(vector.getNumNonZeroElements)

    for(element <- vector.iterateNonZero()){
      out.writeInt(element.index())
      out.writeDouble(element.get())
    }
  }

  def writeBreezeDenseVector(vector: DenseVector[Double], out: DataOutput) = {
    out.writeUTF(breezeDenseVectorId)
    out.writeInt(vector.length)


    for((_, value) <- vector.iterator){
      out.writeDouble(value)
    }
  }

  def readBreezeDenseVector(in: DataInput): DenseVector[Double] = {
    val length = in.readInt()
    val data = new Array[Double](length)

    for(index <- 0 until length){
      val value = in.readDouble()
      data(index) = value
    }

    new DenseVector[Double](data)
  }

  def writeBreezeSparseVector(vector: SparseVector[Double], out: DataOutput) = {
    out.writeUTF(this.breezeSparseVectorId)
    out.writeInt(vector.length)
    out.writeInt(vector.activeSize)

    for((idx, value) <- vector.activeIterator){
      out.writeInt(idx)
      out.writeDouble(value)
    }
  }

  def readBreezeSparseVector(in: DataInput): SparseVector[Double] = {
    val length = in.readInt()
    val used=  in.readInt()

    val builder = new VectorBuilder[Double](length, used)

    for(_ <- 0 until used){
      val idx = in.readInt()
      val value = in.readDouble()
      builder.add(idx, value)
    }

    builder.toSparseVector
  }

  def readMahoutDenseVector(in: DataInput): MahoutDenseVector = {
    val size = in.readInt()
    val data = Array.ofDim[Double](size)

    for(i <- 0 until size){
      val value = in.readDouble()
      data(i) = value
    }

    new MahoutDenseVector(data)
  }

  def readMahoutSparseVector(in: DataInput): MahoutSparseVector = {
    val size = in.readInt()
    val used = in.readInt()

    val result = new MahoutSparseVector(size, used)

    for(_ <- 0 until used){
      val idx = in.readInt()
      val value=  in.readDouble()
      result.setQuick(idx, value)
    }

    result
  }
}