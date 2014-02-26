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
  implicit def opSM_SM[@expand.args(OpAdd, OpSub, OpDiv, OpMulScalar) Op <: OpType](implicit @expand.sequence[Op](
      { _ + _ }, { _ - _ }, { _ / _ }, { _ :* _ } ) op: Op.Impl2[BreezeVector[Double], BreezeVector[Double], BreezeVector[Double]]): 
      Op.Impl2[GilbertVector, GilbertVector, GilbertVector] = {
    new Op.Impl2[GilbertVector, GilbertVector, GilbertVector] {
      def apply(a: GilbertVector, b: GilbertVector) = {
        GilbertVector(op(a.vector, b.vector))
      }
    }
  }
  
  @expand
  @expand.valify
  implicit def opSM_SMUpdate[@expand.args(OpAdd, OpSub, OpDiv, OpMulScalar) Op <: OpType](implicit @expand.sequence[Op](
      { _ += _ }, { _ -= _ }, { _ :/= _ }, { _ :*= _ }) op: Op.InPlaceImpl2[BreezeVector[Double], BreezeVector[Double]]): 
      Op.InPlaceImpl2[GilbertVector, GilbertVector] = {
    new Op.InPlaceImpl2[GilbertVector, GilbertVector] {
      override def apply(a: GilbertVector, b: GilbertVector):Unit = {
        op(a.vector,b.vector)
      }
    }
  }
}