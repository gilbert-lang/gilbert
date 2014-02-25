package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.linalg.SparseVector
import breeze.linalg.{CSCMatrix => BreezeSparseMatrix}
import scala.language.implicitConversions
import breeze.math.Semiring
import breeze.storage.DefaultArrayValue
import scala.reflect.ClassTag

trait BreezeVectorOps {
  implicit def sparseVector2SparseMatrix[@specialized(Double, Boolean) T: ClassTag: Semiring: DefaultArrayValue](sparseVector: SparseVector[T]) = {
    new SparseVectorDecorator(sparseVector)
  }
  
  class SparseVectorDecorator[@specialized(Double, Boolean) T: ClassTag: Semiring: DefaultArrayValue](val sparseVector: SparseVector[T]){
    def asSparseMatrix: BreezeSparseMatrix[T] = {
      val builder = new BreezeSparseMatrix.Builder[T](sparseVector.length, 1, sparseVector.activeSize)
      for((row, value) <- sparseVector.activeIterator){
        builder.add(row, 0, value)
      }
      
      builder.result
    }
  }
}