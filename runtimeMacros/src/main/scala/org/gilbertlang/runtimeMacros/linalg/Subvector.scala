package org.gilbertlang.runtimeMacros.linalg

import breeze.linalg.{Vector => BreezeVector, VectorLike => BreezeVectorLike}
import org.gilbertlang.runtimeMacros.linalg.operators.SubvectorOps
import breeze.linalg.support.CanZipMapValues
import breeze.linalg.support.CanMapValues
import scala.reflect.ClassTag
import breeze.storage.DefaultArrayValue
import breeze.math.Semiring

case class Subvector[@specialized(Double, Boolean) T]
(vector: GilbertVector[T], index: Int, offset: Int, totalEntries: Int) extends 
  BreezeVector[T] with BreezeVectorLike[T, Subvector[T]]{
  
  def this() = this(null,0,0,0)
  
  def length = vector.length
  
  def repr = this
  
  def activeSize = vector.activeSize
  def activeIterator = vector.activeIterator
  def activeValuesIterator = vector.activeValuesIterator
  def activeKeysIterator = vector.activeKeysIterator
  
  def update(i: Int, value: T) = vector.update(i,value)
  
  def apply(i: Int) = vector(i)
  
  def copy = Subvector(vector.copy, index, offset, totalEntries)
  
  
  def asMatrix(implicit classTag: ClassTag[T], semiring: Semiring[T], defaultArrayValue: DefaultArrayValue[T]): 
  Submatrix[T] = {
    Submatrix[T](vector.asMatrix, index, 0, offset, 0, totalEntries, 1)
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
  def apply[@specialized(Double, Boolean) T: DefaultArrayValue: ClassTag]
  (size:Int, index: Int, offset: Int, totalEntries: Int): Subvector[T] = {
    Subvector(GilbertVector[T](size),index, offset, totalEntries)
  }
  
  implicit def canZipMapValues[@specialized(Double, Boolean) T:ClassTag:DefaultArrayValue]: 
  CanZipMapValues[Subvector[T], T, T, Subvector[T]] = {
    new CanZipMapValues[Subvector[T], T, T, Subvector[T]]{
      override def map(a: Subvector[T], b: Subvector[T], fn: (T, T) => T) = {
        val mapper = implicitly[CanZipMapValues[GilbertVector[T], T, T, GilbertVector[T]]]
        val result = mapper.map(a.vector, b.vector, fn)
        Subvector(result, a.index, a.offset, a.totalEntries)
      }
    }
  }
  
  implicit def handholdCMV[T]: CanMapValues.HandHold[Subvector[T], T] = new CanMapValues.HandHold[Subvector[T], T]
}

