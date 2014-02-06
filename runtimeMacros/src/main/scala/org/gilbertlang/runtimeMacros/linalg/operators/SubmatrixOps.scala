package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.macros.expand
import breeze.linalg.operators.OpAdd
import breeze.linalg.operators.OpSub
import breeze.linalg.operators.OpDiv
import breeze.linalg.operators.OpMulScalar
import org.gilbertlang.runtimeMacros.linalg.Submatrix
import breeze.linalg.operators.OpMulMatrix
import org.gilbertlang.runtimeMacros.linalg.Subvector

trait SubmatrixOps {
  @expand
  @expand.valify
  implicit def opSM_SM[@expand.args(OpAdd, OpSub, OpDiv, OpMulScalar) Op](implicit @expand.sequence[Op](
      { _ + _ }, { _ - _ }, { _ / _ }, { _ :* _ }) op: Op.Impl2[GilbertMatrix, GilbertMatrix, GilbertMatrix]): 
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

  implicit val mulSM_SM: OpMulMatrix.Impl2[Submatrix, Submatrix, Submatrix] = {
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
  implicit def opSM_S[@expand.args(OpAdd, OpSub, OpDiv, OpMulMatrix, OpMulScalar) Op](implicit 
      @expand.sequence[Op]({ _ + _ }, { _ - _ }, { _ / _ }, { _ * _ }, { _ * _ }) op: 
      Op.Impl2[GilbertMatrix, Double, GilbertMatrix]): Op.Impl2[Submatrix, Double, Submatrix] = {
    new Op.Impl2[Submatrix, Double, Submatrix] {
      def apply(a: Submatrix, b: Double) = {
        import a._
        Submatrix(op(a.matrix, b), rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
      }
    }
  }
}