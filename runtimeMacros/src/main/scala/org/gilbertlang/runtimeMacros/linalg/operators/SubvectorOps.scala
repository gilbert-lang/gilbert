package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.macros.expand
import breeze.linalg.operators.OpAdd
import breeze.linalg.operators.OpSub
import breeze.linalg.operators.OpDiv
import breeze.linalg.operators.OpMulScalar
import org.gilbertlang.runtimeMacros.linalg.Subvector

trait SubvectorOps {
  this: Subvector.type => 
    
  @expand
  @expand.valify
  implicit def opSM_SM[@expand.args(Double) T, @expand.args(OpAdd, OpSub, OpDiv, OpMulScalar) Op]
  (implicit @expand.sequence[Op]({ _ + _ }, { _ - _ }, { _ / _ }, { _ :* _ }) op: Op.Impl2[GilbertVector[T], 
    GilbertVector[T], GilbertVector[T]]): 
      Op.Impl2[Subvector[T], Subvector[T], Subvector[T]] = {
    new Op.Impl2[Subvector[T], Subvector[T], Subvector[T]] {
      def apply(a: Subvector[T], b: Subvector[T]) = {
        require((a.index, a.offset, a.totalEntries) ==
          (b.index, b.offset, b.totalEntries),
          "Submatrix meta data has to be equal")

        Subvector(op(a.vector, b.vector), a.index, a.offset, a.totalEntries)
      }
    }
  }
  
  @expand
  @expand.valify
  implicit def opSM_SMUpdate[@expand.args(Double) T, @expand.args(OpAdd, OpSub, OpDiv, OpMulScalar) Op]
  (implicit @expand.sequence[Op]({ _ += _ }, { _ -= _ }, { _ :/= _ }, { _ :*= _ }) op: 
      Op.InPlaceImpl2[GilbertVector[T], GilbertVector[T]]): 
      Op.InPlaceImpl2[Subvector[T], Subvector[T]] = {
    new Op.InPlaceImpl2[Subvector[T], Subvector[T]] {
      override def apply(a: Subvector[T], b: Subvector[T]):Unit = {
        op(a.vector,b.vector)
      }
    }
  }
}