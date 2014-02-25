package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.macros.expand
import breeze.linalg.operators.OpAdd
import breeze.linalg.operators.OpSub
import breeze.linalg.operators.OpDiv
import breeze.linalg.operators.OpMulScalar
import org.gilbertlang.runtimeMacros.linalg.GilbertVector
import breeze.linalg.operators.OpMulMatrix
import breeze.linalg.operators.OpType

trait GilbertVectorOps {
  @expand
  @expand.valify
  implicit def opSM_SM[@expand.args(Double) T, @expand.args(OpAdd, OpSub, OpDiv, OpMulScalar) Op <: OpType](implicit @expand.sequence[Op](
      { _ + _ }, { _ - _ }, { _ / _ }, { _ :* _ } ) op: Op.Impl2[BreezeVector[T], BreezeVector[T], BreezeVector[T]]): 
      Op.Impl2[GilbertVector[T], GilbertVector[T], GilbertVector[T]] = {
    new Op.Impl2[GilbertVector[T], GilbertVector[T], GilbertVector[T]] {
      def apply(a: GilbertVector[T], b: GilbertVector[T]) = {
        GilbertVector(op(a.vector, b.vector))
      }
    }
  }
  
  @expand
  @expand.valify
  implicit def opSM_SMUpdate[@expand.args(Double) T, @expand.args(OpAdd, OpSub, OpDiv, OpMulScalar) Op <: OpType](implicit @expand.sequence[Op](
      { _ += _ }, { _ -= _ }, { _ :/= _ }, { _ :*= _ }) op: Op.InPlaceImpl2[BreezeVector[T], BreezeVector[T]]): 
      Op.InPlaceImpl2[GilbertVector[T], GilbertVector[T]] = {
    new Op.InPlaceImpl2[GilbertVector[T], GilbertVector[T]] {
      override def apply(a: GilbertVector[T], b: GilbertVector[T]):Unit = {
        op(a.vector,b.vector)
      }
    }
  }
}