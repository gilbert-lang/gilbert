package org.gilbertlang.runtimeMacros.linalg.breeze.operators

import breeze.linalg.SparseVector
import breeze.linalg.{CSCMatrix => BreezeSparseMatrix}
import org.gilbertlang.runtimeMacros.linalg.breeze.Bitmatrix
import scala.language.implicitConversions
import breeze.math.Semiring
import scala.reflect.ClassTag
import breeze.linalg.BitVector

trait BreezeVectorImplicits {
  implicit def sparseVector2SparseMatrix[T:ClassTag:Semiring](sparseVector: SparseVector[T]) = {
    new SparseVectorDecorator(sparseVector)
  }

  class SparseVectorDecorator[T:ClassTag:Semiring](val sparseVector: SparseVector[T]){
    def asSparseMatrix: BreezeSparseMatrix[T] = {
      val builder = new BreezeSparseMatrix.Builder[T](sparseVector.length, 1, sparseVector.activeSize)
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
        result.update(index, 1, value = true)
      }

      result
    }
  }
}