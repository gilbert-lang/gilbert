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
import breeze.linalg.DenseMatrix
import breeze.linalg.Matrix
import org.gilbertlang.runtimeMacros.linalg.io.DataWriter
import org.gilbertlang.runtimeMacros.linalg.io.DataReader
import scala.reflect.ClassTag
import breeze.math.Semiring
import breeze.storage.DefaultArrayValue

trait GilbertMatrixOps{
  this: GilbertMatrix.type =>
  
  @expand
  @expand.valify
  implicit def opGM_GM[@expand.args(Double) T, @expand.args(OpAdd, OpSub, OpDiv, OpMulScalar) Op]
  (implicit @expand.sequence[Op]({_ + _}, {_ - _}, {_ / _}, {_ :* _}) op: 
      Op.Impl2[BreezeMatrix[T],BreezeMatrix[T],BreezeMatrix[T]]): 
      Op.Impl2[GilbertMatrix[T], GilbertMatrix[T], GilbertMatrix[T]] = {
    new Op.Impl2[GilbertMatrix[T], GilbertMatrix[T], GilbertMatrix[T]]{
      def apply(a: GilbertMatrix[T], b: GilbertMatrix[T]): GilbertMatrix[T] = {
        GilbertMatrix(op(a.matrix, b.matrix))
      }
    }
  }
  
  @expand
  @expand.valify
  implicit def GilbertMatrixMulGilbertMatrix[@expand.args(Double) T]:
  OpMulMatrix.Impl2[GilbertMatrix[T],GilbertMatrix[T], GilbertMatrix[T]] = 
    new OpMulMatrix.Impl2[GilbertMatrix[T],GilbertMatrix[T], GilbertMatrix[T]] {
    def apply(a: GilbertMatrix[T], b: GilbertMatrix[T]): GilbertMatrix[T] = {
      val result: Matrix[T] = (a.matrix, b.matrix) match {
        case (x: DenseMatrix[T], y: DenseMatrix[T]) => x * y
        case (x: Matrix[T], y: Matrix[T]) => x * y
      }
      GilbertMatrix(result)
    }
  }
  
  @expand
  @expand.valify
  implicit def opGM_S[@expand.args(Double) T, @expand.args(OpAdd, OpSub, OpDiv, OpMulMatrix, OpMulScalar) Op]
  (implicit @expand.sequence[Op]({_ + _}, {_ - _}, {_ / _}, {_ * _}, {_ * _}) op: 
      Op.Impl2[BreezeMatrix[T],T,BreezeMatrix[T]]): 
      Op.Impl2[GilbertMatrix[T], T, GilbertMatrix[T]] = {
    new Op.Impl2[GilbertMatrix[T], T, GilbertMatrix[T]] {
      def apply(a: GilbertMatrix[T], b: T) = {
        GilbertMatrix(op(a.matrix, b))
      }
    }
  }
}