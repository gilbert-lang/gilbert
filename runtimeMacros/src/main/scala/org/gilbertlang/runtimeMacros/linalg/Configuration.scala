package org.gilbertlang.runtimeMacros.linalg

import breeze.linalg.{Matrix => BreezeMatrix, CSCMatrix => BreezeSparseMatrix, DenseMatrix => BreezeDenseMatrix,
  Vector => BreezeVector, DenseVector => BreezeDenseVector, SparseVector => BreezeSparseVector}
import breeze.linalg.{CSCMatrix => BreezeSparseMatrix}
import breeze.linalg.{DenseMatrix => BreezeDenseMatrix}
import breeze.linalg.{Matrix => BreezeMatrix}

object Configuration {
  val BLOCKSIZE = 10
  val DENSITYTHRESHOLD = 0.6
  
  type DenseMatrix = BreezeDenseMatrix[Double]
  type SparseMatrix = BreezeSparseMatrix[Double]
  type Matrix = BreezeMatrix[Double]
  
  type Vector = BreezeVector[Double]
  type SparseVector = BreezeSparseVector[Double]
  type DenseVector=  BreezeDenseVector[Double]
  
  def newDenseMatrix(rows: Int, cols: Int, initialValue: Double = 0) = {
    val data = Array.fill[Double](rows*cols)( initialValue)
    new DenseMatrix(rows, cols, data)
  }
  
  def newSparseMatrix(rows: Int, cols: Int, initialNonZero: Int = 0) = {
    BreezeSparseMatrix.zeros[Double](rows, cols, initialNonZero)
  }
  
  def eyeDenseMatrix(rows: Int, cols: Int) = {
    val result = BreezeDenseMatrix.zeros[Double](rows, cols)
    
    for(idx <- 0 until math.min(rows, cols)){
      result.update(idx,idx, 1)
    }
    
    result
  }
  
  def eyeSparseMatrix(rows: Int, cols: Int) = {
    val builder = new BreezeSparseMatrix.Builder[Double](rows, cols, math.min(rows, cols))
    
    for(idx <- 0 until math.min(rows, cols)){
      builder.add(idx,idx,1)
    }
    
    builder.result
  }
}