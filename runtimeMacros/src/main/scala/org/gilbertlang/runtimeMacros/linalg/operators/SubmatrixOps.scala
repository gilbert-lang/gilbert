package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.macros.expand
import breeze.linalg.operators.OpAdd
import breeze.linalg.operators.OpSub
import breeze.linalg.operators.OpDiv
import breeze.linalg.operators.OpMulScalar
import breeze.linalg.operators.{OpGT, OpGTE, OpLT, OpLTE, OpEq, OpNe}
import breeze.linalg.operators.{OpAnd, OpOr}
import org.gilbertlang.runtimeMacros.linalg.Submatrix
import breeze.linalg.operators.OpMulMatrix
import org.gilbertlang.runtimeMacros.linalg.Subvector
import org.gilbertlang.runtimeMacros.linalg.io.DataWriter
import org.gilbertlang.runtimeMacros.linalg.io.DataReader
import scala.reflect.ClassTag
import breeze.math.Semiring
import breeze.storage.DefaultArrayValue

trait SubmatrixOps {
    
  @expand
  @expand.valify
  implicit def opSM_SM[@expand.args(Double) T, @expand.args(OpAdd, OpSub, OpDiv, OpMulScalar) Op]
  (implicit @expand.sequence[Op]({ _ + _ }, { _ - _ }, { _ / _ }, { _ :* _ }) op:
      Op.Impl2[GilbertMatrix[T], GilbertMatrix[T], GilbertMatrix[T]]): 
      Op.Impl2[Submatrix[T], Submatrix[T], Submatrix[T]] = {
    new Op.Impl2[Submatrix[T], Submatrix[T], Submatrix[T]] {
      def apply(a: Submatrix[T], b: Submatrix[T]) = {
        require((a.rowIndex, a.columnIndex, a.rowOffset, a.columnOffset, a.totalRows, a.totalColumns) ==
          (b.rowIndex, b.columnIndex, b.rowOffset, b.columnOffset, b.totalRows, b.totalColumns),
          "Submatrix meta data has to be equal")

        Submatrix(op(a.matrix, b.matrix), a.rowIndex, a.columnIndex, a.rowOffset, a.columnOffset, a.totalRows,
          a.totalColumns)
      }
    }
  }

   @expand
  @expand.valify
  implicit def compOpSM_SM[@expand.args(Double) T, @expand.args(OpGT, OpGTE, OpLT, OpLTE, OpEq, OpNe) Op]
  (implicit @expand.sequence[Op]( {_ :> _}, {_ :>= _}, {_ :< _}, {_ :<= _}, {_ :== _}, {_ :!= _}) op:
      Op.Impl2[GilbertMatrix[T], GilbertMatrix[T], GilbertMatrix[Boolean]]): 
      Op.Impl2[Submatrix[T], Submatrix[T], Submatrix[Boolean]] = {
    new Op.Impl2[Submatrix[T], Submatrix[T], Submatrix[Boolean]] {
      def apply(a: Submatrix[T], b: Submatrix[T]) = {
        require((a.rowIndex, a.columnIndex, a.rowOffset, a.columnOffset, a.totalRows, a.totalColumns) ==
          (b.rowIndex, b.columnIndex, b.rowOffset, b.columnOffset, b.totalRows, b.totalColumns),
          "Submatrix meta data has to be equal")

        Submatrix(op(a.matrix, b.matrix), a.rowIndex, a.columnIndex, a.rowOffset, a.columnOffset, a.totalRows,
          a.totalColumns)
      }
    }
  }
  
  @expand
  @expand.valify
  implicit def logicalOpSM_SM[@expand.args(Boolean) T, @expand.args(OpAnd, OpOr) Op]
  (implicit @expand.sequence[Op]({ _ :& _ }, { _ :| _ }) op:
      Op.Impl2[GilbertMatrix[T], GilbertMatrix[T], GilbertMatrix[T]]): 
      Op.Impl2[Submatrix[T], Submatrix[T], Submatrix[T]] = {
    new Op.Impl2[Submatrix[T], Submatrix[T], Submatrix[T]] {
      def apply(a: Submatrix[T], b: Submatrix[T]) = {
        require((a.rowIndex, a.columnIndex, a.rowOffset, a.columnOffset, a.totalRows, a.totalColumns) ==
          (b.rowIndex, b.columnIndex, b.rowOffset, b.columnOffset, b.totalRows, b.totalColumns),
          "Submatrix meta data has to be equal")

        Submatrix(op(a.matrix, b.matrix), a.rowIndex, a.columnIndex, a.rowOffset, a.columnOffset, a.totalRows,
          a.totalColumns)
      }
    }
  }
  
  @expand
  @expand.valify
  implicit def mulSM_SM[@expand.args(Double) T]: 
  OpMulMatrix.Impl2[Submatrix[T], Submatrix[T], Submatrix[T]] = {
    new OpMulMatrix.Impl2[Submatrix[T], Submatrix[T], Submatrix[T]] {
      def apply(a: Submatrix[T], b: Submatrix[T]) = {
        require(a.cols == b.rows, "Submatrices cannot be multiplied due to unfitting dimensions.")
        require(a.columnIndex == b.rowIndex, "Submatrices are not fitting pairs for multiplication.")
        require(a.totalColumns == b.totalRows, "Matrices don't have the same total columns and rows respectively.")
        Submatrix(a.matrix * b.matrix, a.rowIndex, b.columnIndex, a.rowOffset, b.columnOffset, a.totalRows,
          b.totalColumns)
      }
    }
  }

  @expand
  @expand.valify
  implicit def opSM_S[@expand.args(Double) T, @expand.args(OpAdd, OpSub, OpDiv, OpMulMatrix, OpMulScalar) Op]
  (implicit @expand.sequence[Op]({ _ + _ }, { _ - _ }, { _ / _ }, { _ * _ }, { _ * _ }) op: 
      Op.Impl2[GilbertMatrix[T], T, GilbertMatrix[T]]): Op.Impl2[Submatrix[T], T, Submatrix[T]] = {
    new Op.Impl2[Submatrix[T], T, Submatrix[T]] {
      def apply(a: Submatrix[T], b: T) = {
        import a._
        Submatrix(op(a.matrix, b), rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
      }
    }
  }

  @expand
  @expand.valify
  implicit def compOpSM_S[@expand.args(Double) T, @expand.args(OpGT, OpGTE, OpLT, OpLTE, OpEq, OpNe) Op]
  (implicit @expand.sequence[Op]({_ :> _}, {_ :>= _}, {_ :< _}, {_ :<= _}, {_ :== _}, {_ :!= _}) op: 
      Op.Impl2[GilbertMatrix[T], T, GilbertMatrix[Boolean]]): Op.Impl2[Submatrix[T], T, Submatrix[Boolean]] = {
    new Op.Impl2[Submatrix[T], T, Submatrix[Boolean]] {
      def apply(a: Submatrix[T], b: T) = {
        import a._
        Submatrix(op(a.matrix, b), rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
      }
    }
  }
  
  @expand
  @expand.valify
  implicit def logicalOpSM_S[@expand.args(Boolean) T, @expand.args(OpAnd, OpOr) Op]
  (implicit @expand.sequence[Op]({ _ :& _ }, { _ :| _ }) op: 
      Op.Impl2[GilbertMatrix[T], T, GilbertMatrix[T]]): Op.Impl2[Submatrix[T], T, Submatrix[T]] = {
    new Op.Impl2[Submatrix[T], T, Submatrix[T]] {
      def apply(a: Submatrix[T], b: T) = {
        import a._
        Submatrix(op(a.matrix, b), rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
      }
    }
  }
}