package org.gilbertlang.runtimeMacros.linalg

import breeze.linalg.{Vector => BreezeVector, VectorLike => BreezeVectorLike}
import org.gilbertlang.runtimeMacros.linalg.operators.SubvectorOps
import breeze.linalg.support.CanZipMapValues
import breeze.linalg.support.CanMapValues
import scala.reflect.ClassTag
import breeze.storage.DefaultArrayValue
import breeze.math.Semiring

case class Subvector(vector: GilbertVector, index: Int, offset: Int, totalEntries: Int) extends 
  BreezeVector[Double] with BreezeVectorLike[Double, Subvector]{
  
  def this() = this(null,0,0,0)
  
  def length = vector.length
  
  def repr = this
  
  def activeSize = vector.activeSize
  def activeIterator = vector.activeIterator
  def activeValuesIterator = vector.activeValuesIterator
  def activeKeysIterator = vector.activeKeysIterator
  
  def update(i: Int, value: Double) = vector.update(i,value)
  
  def apply(i: Int) = vector(i)
  
  def copy = Subvector(vector.copy, index, offset, totalEntries)
  
  
  def asMatrix:Submatrix = {
    Submatrix(vector.asMatrix, index, 0, offset, 0, totalEntries, 1)
  }
  
  override def toString: String = {
    val subvectorInfo = s"Subvectr[Index: $index, Offset: $offset, TotalEntries: $totalEntries]"
    
    if(vector != null)
      subvectorInfo + "\n"+ vector
    else
        subvectorInfo
  }
}

object Subvector extends SubvectorOps{
  def apply(size:Int, index: Int, offset: Int, totalEntries: Int): Subvector = {
    Subvector(GilbertVector(size),index, offset, totalEntries)
  }
  
  implicit def canZipMapValues:CanZipMapValues[Subvector, Double, Double, Subvector] = {
    new CanZipMapValues[Subvector, Double, Double, Subvector]{
      override def map(a: Subvector, b: Subvector, fn: (Double, Double) => Double) = {
        val mapper = implicitly[CanZipMapValues[GilbertVector, Double, Double, GilbertVector]]
        val result = mapper.map(a.vector, b.vector, fn)
        Subvector(result, a.index, a.offset, a.totalEntries)
      }
    }
  }
  
  implicit def handholdCMV[T]: CanMapValues.HandHold[Subvector, Double] = new CanMapValues.HandHold[Subvector, Double]
}

