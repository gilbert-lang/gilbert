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
import breeze.linalg.operators.OpNe
import breeze.linalg.operators.OpGT
import breeze.linalg.operators.OpGTE
import breeze.linalg.operators.OpLT
import breeze.linalg.operators.OpLTE
import breeze.linalg.operators.OpEq
import breeze.linalg.operators.OpAnd
import breeze.linalg.operators.OpOr

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
  implicit def compOpGM_GM[@expand.args(Double) T, @expand.args(OpGT, OpGTE, OpLT, OpLTE, OpEq, OpNe) Op]
  (implicit @expand.sequence[Op]({_ :> _}, {_ :>= _}, {_ :< _}, {_ :<= _}, {_ :== _}, {_ :!= _}) op: 
    Op.Impl2[BreezeMatrix[T], BreezeMatrix[T], BreezeMatrix[Boolean]]):
  Op.Impl2[GilbertMatrix[T], GilbertMatrix[T], GilbertMatrix[Boolean]] =
  new Op.Impl2[GilbertMatrix[T], GilbertMatrix[T], GilbertMatrix[Boolean]]{
    override def apply(a: GilbertMatrix[T], b: GilbertMatrix[T]): GilbertMatrix[Boolean] = {
      GilbertMatrix(op(a.matrix, b.matrix))
    }
  }
  
  @expand
  @expand.valify
  implicit def logicalOpGM_GM[@expand.args(Boolean) T, @expand.args(OpAnd, OpOr) Op](implicit @expand.sequence[Op]({_ :& _}, 
      {_ :| _}) op: Op.Impl2[BreezeMatrix[T],BreezeMatrix[T],BreezeMatrix[T]]): 
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

  @expand
  @expand.valify
  implicit def compOpGM_S[@expand.args(Double) T, @expand.args(OpGT, OpGTE, OpLT, OpLTE, OpEq, OpNe) Op]
  (implicit @expand.sequence[Op]({_ :> _}, {_ :>= _}, {_ :< _}, {_ :<= _}, {_ :== _}, {_ :!= _}) op: 
    Op.Impl2[BreezeMatrix[T], T, BreezeMatrix[Boolean]]):
  Op.Impl2[GilbertMatrix[T], T, GilbertMatrix[Boolean]] =
  new Op.Impl2[GilbertMatrix[T], T, GilbertMatrix[Boolean]]{
    override def apply(a: GilbertMatrix[T], b: T): GilbertMatrix[Boolean] = {
      GilbertMatrix(op(a.matrix, b))
    }
  }
  
  @expand
  @expand.valify
  implicit def logicalOpGM_S[@expand.args(Boolean) T, @expand.args(OpAnd, OpOr) Op](implicit @expand.sequence[Op]({_ :& _}, 
      {_ :| _}) op: 
      Op.Impl2[BreezeMatrix[T],T,BreezeMatrix[T]]): 
      Op.Impl2[GilbertMatrix[T],T, GilbertMatrix[T]] = {
    new Op.Impl2[GilbertMatrix[T], T, GilbertMatrix[T]]{
      def apply(a: GilbertMatrix[T], b:T): GilbertMatrix[T] = {
        GilbertMatrix(op(a.matrix, b))
      }
    }
  }
}