package org.gilbertlang.runtimeMacros.linalg

import breeze.linalg.{Vector=>BreezeVector, VectorLike => BreezeVectorLike,
  DenseVector => BreezeDenseVector, SparseVector => BreezeSparseVector}
import eu.stratosphere.types.Value
import java.io.DataOutput
import java.io.DataInput
import org.gilbertlang.runtimeMacros.linalg.operators.GilbertVectorOps
import breeze.linalg.support.CanZipMapValues
import org.gilbertlang.runtimeMacros.linalg.operators.BreezeVectorOps

class GilbertVector(var vector: BreezeVector[Double]) extends BreezeVector[Double] with 
BreezeVectorLike[Double, GilbertVector] with Value with BreezeVectorOps {
  def this() = this(null)
  
  def write(out: DataOutput){
    VectorSerialization.write(vector,out)
  }
  
  def read(in: DataInput){
    vector = VectorSerialization.read(in)
  }
  
  def length = vector.length
 
  def repr = this
  
  def activeSize = vector.activeSize
  def activeIterator = vector.activeIterator
  def activeValuesIterator = vector.activeValuesIterator
  def activeKeysIterator = vector.activeKeysIterator
  
  def apply(i: Int) = vector(i)
  
  def update(i: Int, value: Double){
    vector.update(i,value)
  }
  
  def copy = GilbertVector(vector.copy)
  
  def asMatrix: GilbertMatrix = {
    val result = vector match {
      case x: BreezeDenseVector[Double] => x.asDenseMatrix
      case x: BreezeSparseVector[Double] => x.asSparseMatrix
    }
    GilbertMatrix(result)
  }
}

object GilbertVector extends GilbertVectorOps {
  def apply(vector: BreezeVector[Double]): GilbertVector = new GilbertVector(vector)
  
  implicit val canZipMapValues: CanZipMapValues[GilbertVector, Double, Double, GilbertVector] = {
    new CanZipMapValues[GilbertVector, Double, Double, GilbertVector]{
      override def map(a: GilbertVector, b: GilbertVector, fn: (Double, Double) => Double) = {
        val result = (a.vector, b.vector) match {
          case (x: BreezeDenseVector[Double], y: BreezeDenseVector[Double]) =>
            val mapper = implicitly[CanZipMapValues[BreezeDenseVector[Double], Double, Double, BreezeDenseVector[Double]]]
            mapper.map(x,y,fn)
          case (x: BreezeSparseVector[Double], y: BreezeSparseVector[Double]) =>
            val mapper = implicitly[CanZipMapValues[BreezeSparseVector[Double], Double, Double, BreezeSparseVector[Double]]]
            mapper.map(x,y,fn)
          case _ =>
            val mapper = implicitly[CanZipMapValues[BreezeVector[Double], Double, Double, BreezeVector[Double]]]
            mapper.map(a.vector,b.vector,fn)
        }
        GilbertVector(result)
      }
    }
  }
}