package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.macros.expand
import breeze.linalg.operators._
import org.gilbertlang.runtimeMacros.linalg.Submatrix
import org.gilbertlang.runtimeMacros.linalg.Subvector
import scala.reflect.ClassTag
import breeze.math.Semiring
import breeze.storage.DefaultArrayValue
import org.gilbertlang.runtimeMacros.linalg.SubmatrixBoolean

trait SubmatrixOps {
    
  @expand
  @expand.valify
  implicit def opSM_SM[@expand.args(OpAdd, OpSub, OpDiv, OpMulScalar) Op]
  (implicit @expand.sequence[Op]({ _ + _ }, { _ - _ }, { _ / _ }, { _ :* _ }) op:
      Op.Impl2[GilbertMatrix, GilbertMatrix, GilbertMatrix]): 
      Op.Impl2[Submatrix, Submatrix, Submatrix] = {
    new Op.Impl2[Submatrix, Submatrix, Submatrix] {
      def apply(a: Submatrix, b: Submatrix) = {
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
  implicit def compOpSM_SM[@expand.args(OpGT, OpGTE, OpLT, OpLTE, OpEq, OpNe) Op]
  (implicit @expand.sequence[Op]( {_ :> _}, {_ :>= _}, {_ :< _}, {_ :<= _}, {_ :== _}, {_ :!= _}) op:
      Op.Impl2[GilbertMatrix, GilbertMatrix, GilbertMatrixBoolean]): 
      Op.Impl2[Submatrix, Submatrix, SubmatrixBoolean] = {
    new Op.Impl2[Submatrix, Submatrix, SubmatrixBoolean] {
      def apply(a: Submatrix, b: Submatrix) = {
        require((a.rowIndex, a.columnIndex, a.rowOffset, a.columnOffset, a.totalRows, a.totalColumns) ==
          (b.rowIndex, b.columnIndex, b.rowOffset, b.columnOffset, b.totalRows, b.totalColumns),
          "Submatrix meta data has to be equal")

        SubmatrixBoolean(op(a.matrix, b.matrix), a.rowIndex, a.columnIndex, a.rowOffset, a.columnOffset, a.totalRows,
          a.totalColumns)
      }
    }
  }
  
  @expand
  @expand.valify
  implicit def mulSM_SM: OpMulMatrix.Impl2[Submatrix, Submatrix, Submatrix] = {
    new OpMulMatrix.Impl2[Submatrix, Submatrix, Submatrix] {
      def apply(a: Submatrix, b: Submatrix) = {
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
  implicit def opSM_S[@expand.args(OpAdd, OpSub, OpDiv, OpMulMatrix, OpMulScalar, OpPow) Op <: OpType]
  (implicit @expand.sequence[Op]({ _ + _ }, { _ - _ }, { _ / _ }, { _ * _ }, { _ * _ }, {_ :^ _}) op:
      Op.Impl2[GilbertMatrix, Double, GilbertMatrix]): Op.Impl2[Submatrix, Double, Submatrix] = {
    new Op.Impl2[Submatrix, Double, Submatrix] {
      def apply(a: Submatrix, b: Double) = {
        import a._
        Submatrix(op(a.matrix, b), rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
      }
    }
  }

  @expand
  @expand.valify
  implicit def compOpSM_S[@expand.args(OpGT, OpGTE, OpLT, OpLTE, OpEq, OpNe) Op]
  (implicit @expand.sequence[Op]({_ :> _}, {_ :>= _}, {_ :< _}, {_ :<= _}, {_ :== _}, {_ :!= _}) op: 
      Op.Impl2[GilbertMatrix, Double, GilbertMatrixBoolean]): Op.Impl2[Submatrix, Double, SubmatrixBoolean] = {
    new Op.Impl2[Submatrix, Double, SubmatrixBoolean] {
      def apply(a: Submatrix, b: Double) = {
        import a._
        SubmatrixBoolean(op(a.matrix, b), rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
      }
    }
  }
}