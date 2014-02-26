package org.gilbertlang.runtimeMacros.linalg

import breeze.linalg.{Vector=>BreezeVector, VectorLike => BreezeVectorLike,
  DenseVector => BreezeDenseVector, SparseVector => BreezeSparseVector}
import eu.stratosphere.types.Value
import java.io.DataOutput
import java.io.DataInput
import org.gilbertlang.runtimeMacros.linalg.operators.GilbertVectorOps
import breeze.linalg.support.CanZipMapValues
import org.gilbertlang.runtimeMacros.linalg.operators.BreezeVectorOps
import org.gilbertlang.runtimeMacros.linalg.io.Serializer
import scala.reflect.ClassTag
import breeze.math.Semiring
import breeze.storage.DefaultArrayValue
import breeze.linalg.support.CanCopy

class GilbertVector[@specialized(Double, Boolean) T](var vector: BreezeVector[T]) extends BreezeVector[T] with 
BreezeVectorLike[T, GilbertVector[T]] with Value with BreezeVectorOps {
  def this() = this(null)
  
  def write(out: DataOutput){
    VectorSerialization.write(vector,out)
  }
  
  def read(in: DataInput){
    vector = VectorSerialization.read[T](in)
  }
  
  def length = vector.length
 
  def repr = this
  
  def activeSize = vector.activeSize
  def activeIterator = vector.activeIterator
  def activeValuesIterator = vector.activeValuesIterator
  def activeKeysIterator = vector.activeKeysIterator
  
  def apply(i: Int) = vector(i)
  
  def update(i: Int, value: T){
    vector.update(i,value)
  }
  
  def copy = GilbertVector[T](this.vector.copy)
  
  def asMatrix(implicit classTag: ClassTag[T], semiring: Semiring[T], defaultArrayValue: DefaultArrayValue[T]): 
  GilbertMatrix[T] = {
    val result = vector match {
      case x: BreezeDenseVector[T] => x.asDenseMatrix
      case x: BreezeSparseVector[T] => x.asSparseMatrix
    }
    GilbertMatrix[T](result)
  }
  
  override def toString: String = vector.toString
}

object GilbertVector extends GilbertVectorOps {
  def apply[@specialized(Double, Boolean) T]
  (vector: BreezeVector[T]): GilbertVector[T] = new GilbertVector[T](vector)
  
  def apply[@specialized(Double, Boolean) T: DefaultArrayValue: ClassTag]
  (size: Int, numNonZeroElements: Int = 0): GilbertVector[T] = {
    val ratio = numNonZeroElements.toDouble/size
    
    val vector = if(ratio > Configuration.DENSITYTHRESHOLD){
      BreezeDenseVector.zeros[T](size)
    }else{
      BreezeSparseVector.zeros[T](size)
    }
    
    new GilbertVector(vector)
  }
  
  implicit def canCopyGilbertVector[@specialized(Double, Boolean) T: ClassTag: DefaultArrayValue]:CanCopy[GilbertVector[T]] = 
    new CanCopy[GilbertVector[T]]{
    override def apply(gilbert: GilbertVector[T]): GilbertVector[T] = {
      GilbertVector[T](breeze.linalg.copy(gilbert.vector))
    }
  }
  
  implicit def canZipMapValues[@specialized(Double, Boolean) T:ClassTag:DefaultArrayValue]: CanZipMapValues[GilbertVector[T], T, T, GilbertVector[T]] = {
    new CanZipMapValues[GilbertVector[T], T, T, GilbertVector[T]]{
      override def map(a: GilbertVector[T], b: GilbertVector[T], fn: (T, T) => T) = {
        val result = (a.vector, b.vector) match {
          case (x: BreezeDenseVector[T], y: BreezeDenseVector[T]) =>
            val mapper = implicitly[CanZipMapValues[BreezeDenseVector[T], T, T, BreezeDenseVector[T]]]
            mapper.map(x,y,fn)
          case (x: BreezeSparseVector[T], y: BreezeSparseVector[T]) =>
            val mapper = implicitly[CanZipMapValues[BreezeSparseVector[T], T, T, BreezeSparseVector[T]]]
            mapper.map(x,y,fn)
          case _ =>
            val mapper = implicitly[CanZipMapValues[BreezeVector[T], T, T, BreezeVector[T]]]
            mapper.map(a.vector,b.vector,fn)
        }
        GilbertVector(result)
      }
    }
  }
}