package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.macros.expand
import breeze.linalg.operators._
import org.gilbertlang.runtimeMacros.linalg.{Bitmatrix, GilbertMatrix, GilbertMatrixBoolean}
import breeze.linalg.DenseMatrix
import breeze.linalg.Matrix

trait GilbertMatrixOps{

  this: GilbertMatrix.type =>
  
  @expand
  @expand.valify
  implicit def opGM_GM[@expand.args(OpAdd, OpSub, OpDiv, OpMulScalar, OpPow) Op]
  (implicit @expand.sequence[Op]({_ + _}, {_ - _}, {_ / _}, {_ :* _}, {_ :^ _}) op:
      Op.Impl2[Matrix[Double],Matrix[Double],Matrix[Double]]): 
      Op.Impl2[GilbertMatrix, GilbertMatrix, GilbertMatrix] = {
    new Op.Impl2[GilbertMatrix, GilbertMatrix, GilbertMatrix]{
      def apply(a: GilbertMatrix, b: GilbertMatrix): GilbertMatrix = {
        GilbertMatrix(op(a.matrix, b.matrix))
      }
    }
  }

  @expand
  @expand.valify
  implicit def compOpGM_GM[@expand.args(OpGT, OpGTE, OpLT, OpLTE, OpEq, OpNe) Op]
  (implicit @expand.sequence[Op]({_ :> _}, {_ :>= _}, {_ :< _}, {_ :<= _}, {_ :== _}, {_ :!= _}) op: 
    Op.Impl2[Matrix[Double], Matrix[Double], Bitmatrix]):
  Op.Impl2[GilbertMatrix, GilbertMatrix, GilbertMatrixBoolean] =
  new Op.Impl2[GilbertMatrix, GilbertMatrix, GilbertMatrixBoolean]{
    override def apply(a: GilbertMatrix, b: GilbertMatrix): GilbertMatrixBoolean = {
      GilbertMatrixBoolean(op(a.matrix, b.matrix))
    }
  }
  
  @expand
  @expand.valify
  implicit def GilbertMatrixMulGilbertMatrix:
  OpMulMatrix.Impl2[GilbertMatrix,GilbertMatrix, GilbertMatrix] = 
    new OpMulMatrix.Impl2[GilbertMatrix,GilbertMatrix, GilbertMatrix] {
    def apply(a: GilbertMatrix, b: GilbertMatrix): GilbertMatrix = {
      val result: Matrix[Double] = (a.matrix, b.matrix) match {
        case (x: DenseMatrix[Double], y: DenseMatrix[Double]) => x * y
        case (x: Matrix[Double], y: Matrix[Double]) => x * y
      }
      GilbertMatrix(result)
    }
  }
  
  @expand
  @expand.valify
  implicit def opGM_S[@expand.args(OpAdd, OpSub, OpDiv, OpMulMatrix, OpMulScalar, OpPow) Op <: OpType]
  (implicit @expand.sequence[Op]({_ + _}, {_ - _}, {_ / _}, {_ * _}, {_ * _}, {_ :^ _}) op:
      Op.Impl2[Matrix[Double],Double,Matrix[Double]]): 
      Op.Impl2[GilbertMatrix, Double, GilbertMatrix] = {
    new Op.Impl2[GilbertMatrix, Double, GilbertMatrix] {
      def apply(a: GilbertMatrix, b: Double) = {
        GilbertMatrix(op(a.matrix, b))
      }
    }
  }

  @expand
  @expand.valify
  implicit def compOpGM_S[@expand.args(OpGT, OpGTE, OpLT, OpLTE, OpEq, OpNe) Op]
  (implicit @expand.sequence[Op]({_ :> _}, {_ :>= _}, {_ :< _}, {_ :<= _}, {_ :== _}, {_ :!= _}) op: 
    Op.Impl2[Matrix[Double], Double, Bitmatrix]):
  Op.Impl2[GilbertMatrix, Double, GilbertMatrixBoolean] =
  new Op.Impl2[GilbertMatrix, Double, GilbertMatrixBoolean]{
    override def apply(a: GilbertMatrix, b: Double): GilbertMatrixBoolean = {
      GilbertMatrixBoolean(op(a.matrix, b))
    }
  }
  
}