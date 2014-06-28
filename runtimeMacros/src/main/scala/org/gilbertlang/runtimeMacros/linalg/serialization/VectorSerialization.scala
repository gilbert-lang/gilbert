package org.gilbertlang.runtimeMacros.linalg.serialization

import java.io.{DataInput, DataOutput}


import breeze.linalg.{VectorBuilder, SparseVector, DenseVector}
import org.gilbertlang.runtimeMacros.linalg.breeze._
import org.gilbertlang.runtimeMacros.linalg.{ DoubleVector}


object VectorSerialization {
  val breezeSparseVectorId = "breezeSparseVector"
  val breezeDenseVectorId = "breezeDenseVector"
  val breezeBitvectorId = "breezeBitvector"
  val mahoutSparseVectorId = "mahoutSparseVector"
  val mahoutDenseVectorId = "mahoutDenseVector"
  val mahoutBooleanVector = "mahoutBooleanVector"

  def readDouble(in: DataInput): DoubleVector = {
    val tpeId = in.readUTF()

    tpeId match {
      case breezeDenseVectorId => readBreezeDenseVector(in)
      case breezeSparseVectorId => readBreezeSparseVector(in)
      case _ =>
        throw new IllegalArgumentException("Vector serialization does not support vectors with type id " + tpeId)
    }
  }

  def writeDouble(vector: DoubleVector, out: DataOutput) = {
    vector match {
      case v: BreezeDoubleVector =>
        writeBreezeDoubleVector(v, out)
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
}