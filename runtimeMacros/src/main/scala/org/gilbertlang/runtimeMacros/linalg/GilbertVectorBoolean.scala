package org.gilbertlang.runtimeMacros.linalg

import breeze.linalg.{Vector=>BreezeVector, VectorLike => BreezeVectorLike,
  DenseVector => BreezeDenseVector, SparseVector => BreezeSparseVector}
import eu.stratosphere.types.Value
import java.io.DataOutput
import java.io.DataInput
import org.gilbertlang.runtimeMacros.linalg.operators.GilbertVectorOps
import breeze.linalg.support.CanZipMapValues
import org.gilbertlang.runtimeMacros.linalg.operators.BreezeVectorImplicits
import org.gilbertlang.runtimeMacros.linalg.io.Serializer
import scala.reflect.ClassTag
import breeze.math.Semiring
import breeze.storage.DefaultArrayValue
import breeze.linalg.support.CanCopy
import breeze.linalg.BitVector

class GilbertVectorBoolean(var vector: BitVector) extends BreezeVector[Boolean] with 
BreezeVectorLike[Boolean, GilbertVectorBoolean] with Value with BreezeVectorImplicits {
  def this() = this(null)
  
  def write(out: DataOutput){
    VectorSerialization.writeBitVector(vector,out)
  }
  
  def read(in: DataInput){
    vector = VectorSerialization.readBitVector(in)
  }
  
  def length = vector.length
 
  def repr = this
  
  def activeSize = vector.activeSize
  def activeIterator = vector.activeIterator
  def activeValuesIterator = vector.activeValuesIterator
  def activeKeysIterator = vector.activeKeysIterator
  
  def apply(i: Int) = vector(i)
  
  def update(i: Int, value: Boolean){
    vector.update(i,value)
  }
  
  def copy = GilbertVectorBoolean(this.vector.copy)
  
  def asMatrix: GilbertMatrixBoolean = {
    GilbertMatrixBoolean(vector.asMatrix)
  }
  
  override def toString: String = vector.toString
}

object GilbertVectorBoolean {
  def apply(vector: BitVector): GilbertVectorBoolean = new GilbertVectorBoolean(vector)
  
  def apply(size: Int, numNonZeroElements: Int = 0): GilbertVectorBoolean = {
        new GilbertVectorBoolean(BitVector.zeros(size))
  }
  
  implicit def canCopyGilbertVectorBoolean:CanCopy[GilbertVectorBoolean] = 
    new CanCopy[GilbertVectorBoolean]{
    override def apply(gilbert: GilbertVectorBoolean): GilbertVectorBoolean = {
      GilbertVectorBoolean(gilbert.vector.copy)
    }
  }
}