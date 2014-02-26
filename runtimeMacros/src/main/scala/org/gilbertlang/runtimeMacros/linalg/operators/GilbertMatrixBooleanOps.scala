package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.macros.expand
import breeze.linalg.operators.OpAnd
import breeze.linalg.operators.OpOr
import org.gilbertlang.runtimeMacros.linalg.GilbertMatrixBoolean
import org.gilbertlang.runtimeMacros.linalg.Bitmatrix

trait GilbertMatrixBooleanOps {
  @expand
  @expand.valify
  implicit def logicalOpGM_S[@expand.args(OpAnd, OpOr) Op](implicit @expand.sequence[Op]({_ :& _}, 
      {_ :| _}) op: 
      Op.Impl2[Bitmatrix,Boolean,Bitmatrix]): 
      Op.Impl2[GilbertMatrixBoolean,Boolean, GilbertMatrixBoolean] = {
    new Op.Impl2[GilbertMatrixBoolean, Boolean, GilbertMatrixBoolean]{
      def apply(a: GilbertMatrixBoolean, b:Boolean): GilbertMatrixBoolean = {
        GilbertMatrixBoolean(op(a.matrix, b))
      }
    }
  }
 
  @expand
  @expand.valify
  implicit def logicalOpGM_GM[@expand.args(OpAnd, OpOr) Op](implicit @expand.sequence[Op]({_ :& _}, 
      {_ :| _}) op: Op.Impl2[Bitmatrix,Bitmatrix,Bitmatrix]): 
      Op.Impl2[GilbertMatrixBoolean, GilbertMatrixBoolean, GilbertMatrixBoolean] = {
    new Op.Impl2[GilbertMatrixBoolean, GilbertMatrixBoolean, GilbertMatrixBoolean]{
      def apply(a: GilbertMatrixBoolean, b: GilbertMatrixBoolean): GilbertMatrixBoolean = {
        GilbertMatrixBoolean(op(a.matrix, b.matrix))
      }
    }
  }
}