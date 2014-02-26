package org.gilbertlang.runtimeMacros.linalg

import breeze.linalg.{Matrix => BreezeMatrix, CSCMatrix => BreezeSparseMatrix, DenseMatrix => BreezeDenseMatrix,
  Vector => BreezeVector, DenseVector => BreezeDenseVector, SparseVector => BreezeSparseVector}
import breeze.linalg.{CSCMatrix => BreezeSparseMatrix}
import breeze.linalg.{DenseMatrix => BreezeDenseMatrix}
import breeze.linalg.{Matrix => BreezeMatrix}
import scala.reflect.ClassTag
import breeze.storage.DefaultArrayValue
import breeze.util.ArrayUtil
import breeze.math.Semiring

object Configuration {
  val BLOCKSIZE = 10
  val DENSITYTHRESHOLD = 0.6
  
  type DenseMatrix = BreezeDenseMatrix[Double]
  type SparseMatrix = BreezeSparseMatrix[Double]
  type Matrix = BreezeMatrix[Double]
  
  type Vector = BreezeVector[Double]
  type SparseVector = BreezeSparseVector[Double]
  type DenseVector=  BreezeDenseVector[Double]
  
  def newDenseMatrix[@specialized(Double, Boolean) T: ClassTag: DefaultArrayValue](rows: Int, cols: Int) = {
    val data = new Array[T](rows*cols)
    if(implicitly[DefaultArrayValue[T]] != null && rows* cols != 0 && data(0) != implicitly[DefaultArrayValue[T]].value)
      ArrayUtil.fill(data,0, data.length, implicitly[DefaultArrayValue[T]].value)
    new BreezeDenseMatrix[T](rows, cols, data)
  }
  
  def newDenseMatrix[@specialized(Double, Boolean) T: ClassTag](rows: Int, cols: Int, initialValue: T) = {
    val data = Array.fill[T](rows*cols)(initialValue)
    new BreezeDenseMatrix[T](rows, cols, data)
  }
  
  def newSparseMatrix[@specialized(Double, Boolean) T: ClassTag: DefaultArrayValue](rows: Int, cols: Int) = {
    BreezeSparseMatrix.zeros[T](rows, cols)
  }
  
  def eyeDenseMatrix[@specialized(Double, Boolean) T: ClassTag: Semiring: DefaultArrayValue](rows: Int, cols: Int) = {
    val result = BreezeDenseMatrix.zeros[T](rows, cols)
    val ring = implicitly[Semiring[T]]
    for(idx <- 0 until math.min(rows, cols)){
      result.update(idx,idx, ring.one)
    }
    
    result
  }
  
  def eyeSparseMatrix[@specialized(Double, Boolean) T: ClassTag: Semiring: DefaultArrayValue]
  (rows: Int, cols: Int) = {
    val builder = new BreezeSparseMatrix.Builder[T](rows, cols, math.min(rows, cols))
    val ring = implicitly[Semiring[T]]
    for(idx <- 0 until math.min(rows, cols)){
      builder.add(idx,idx,ring.one)
    }
    
    builder.result
  }
}