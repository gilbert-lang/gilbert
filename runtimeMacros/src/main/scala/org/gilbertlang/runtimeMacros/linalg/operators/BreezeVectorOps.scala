package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.linalg.SparseVector
import breeze.linalg.CSCMatrix

trait BreezeVectorOps {
  implicit def sparseVector2SparseMatrix(sparseVector: SparseVector[Double]) = {
    new SparseVectorDecorator(sparseVector)
  }
  
  class SparseVectorDecorator(val sparseVector: SparseVector[Double]){
    def asSparseMatrix: CSCMatrix[Double] = {
      val builder = new CSCMatrix.Builder[Double](sparseVector.length, 1, sparseVector.activeSize)
      for((row, value) <- sparseVector.activeIterator){
        builder.add(row, 0, value)
      }
      
      builder.result
    }
  }
}