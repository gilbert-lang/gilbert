package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.macros.expand
import breeze.linalg.operators.OpAdd
import breeze.linalg.operators.OpSub
import breeze.linalg.operators.OpDiv
import breeze.linalg.operators.OpMulMatrix
import breeze.linalg.operators.OpMulScalar
import org.gilbertlang.runtimeMacros.linalg.GilbertMatrix
import org.gilbertlang.runtimeMacros.linalg.Configuration
import org.gilbertlang.runtimeMacros.linalg.GilbertVector

trait GilbertMatrixOps{
  @expand
  @expand.valify
  implicit def opGM_GM[@expand.args(OpAdd, OpSub, OpDiv, OpMulMatrix, OpMulScalar) Op]
  (implicit @expand.sequence[Op]({_ + _}, {_ - _}, {_ / _}, {_ * _}, {_ :* _}) op: 
      Op.Impl2[BreezeMatrix[Double],BreezeMatrix[Double],BreezeMatrix[Double]]): 
      Op.Impl2[GilbertMatrix, GilbertMatrix, GilbertMatrix] = {
    new Op.Impl2[GilbertMatrix, GilbertMatrix, GilbertMatrix]{
      def apply(a: GilbertMatrix, b: GilbertMatrix): GilbertMatrix = {
        GilbertMatrix(op(a.matrix, b.matrix))
      }
    }
  }
  
  @expand
  @expand.valify
  implicit def opGM_S[@expand.args(OpAdd, OpSub, OpDiv, OpMulMatrix, OpMulScalar) Op]
  (implicit @expand.sequence[Op]({_ + _}, {_ - _}, {_ / _}, {_ * _}, {_ * _}) op: 
      Op.Impl2[BreezeMatrix[Double],Double,BreezeMatrix[Double]]): 
      Op.Impl2[GilbertMatrix, Double, GilbertMatrix] = {
    new Op.Impl2[GilbertMatrix, Double, GilbertMatrix] {
      def apply(a: GilbertMatrix, b: Double) = {
        GilbertMatrix(op(a.matrix, b))
      }
    }
  }
}