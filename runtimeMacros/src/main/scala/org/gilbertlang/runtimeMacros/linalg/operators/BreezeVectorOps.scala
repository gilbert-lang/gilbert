package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.linalg.SparseVector
import breeze.linalg.{CSCMatrix => BreezeSparseMatrix}
import scala.language.implicitConversions
import breeze.math.Semiring
import breeze.storage.DefaultArrayValue
import scala.reflect.ClassTag
import breeze.linalg.BitVector
import org.gilbertlang.runtimeMacros.linalg.Bitmatrix

trait BreezeVectorOps {
  implicit def sparseVector2SparseMatrix(sparseVector: SparseVector[Double]) = {
    new SparseVectorDecorator(sparseVector)
  }
  
  class SparseVectorDecorator(val sparseVector: SparseVector[Double]){
    def asSparseMatrix: BreezeSparseMatrix[Double] = {
      val builder = new BreezeSparseMatrix.Builder[Double](sparseVector.length, 1, sparseVector.activeSize)
      for((row, value) <- sparseVector.activeIterator){
        builder.add(row, 0, value)
      }
      
      builder.result
    }
  }
  
  implicit def bitVector2Bitmatrix(bitvector: BitVector) = {
    new BitVectorDecorator(bitvector)
  }
  
  class BitVectorDecorator(val bitvector: BitVector){
    def asMatrix: Bitmatrix = {
      val result = new Bitmatrix(bitvector.size, 1)
      
      for(index <- bitvector.activeKeysIterator){
        result.update(index, 1, true)
      }
      
      result
    }
  }
}