package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.macros.expand
import breeze.linalg.operators.OpAnd
import breeze.linalg.operators.OpOr
import org.gilbertlang.runtimeMacros.linalg.SubmatrixBoolean

trait SubmatrixBooleanOps {
  @expand
  @expand.valify
  implicit def logicalOpSM_S[ @expand.args(OpAnd, OpOr) Op]
  (implicit @expand.sequence[Op]({ _ :& _ }, { _ :| _ }) op: 
      Op.Impl2[GilbertMatrixBoolean, Boolean, GilbertMatrixBoolean]): Op.Impl2[SubmatrixBoolean, Boolean, 
        SubmatrixBoolean] = {
    new Op.Impl2[SubmatrixBoolean, Boolean, SubmatrixBoolean] {
      def apply(a: SubmatrixBoolean, b: Boolean) = {
        import a._
        SubmatrixBoolean(op(a.matrix, b), rowIndex, columnIndex, rowOffset, columnOffset, totalRows, totalColumns)
      }
    }
  }
 
  @expand
  @expand.valify
  implicit def logicalOpSM_SM[@expand.args(OpAnd, OpOr) Op]
  (implicit @expand.sequence[Op]({ _ :& _ }, { _ :| _ }) op:
      Op.Impl2[GilbertMatrixBoolean, GilbertMatrixBoolean, GilbertMatrixBoolean]): 
      Op.Impl2[SubmatrixBoolean, SubmatrixBoolean, SubmatrixBoolean] = {
    new Op.Impl2[SubmatrixBoolean, SubmatrixBoolean, SubmatrixBoolean] {
      def apply(a: SubmatrixBoolean, b: SubmatrixBoolean) = {
        require((a.rowIndex, a.columnIndex, a.rowOffset, a.columnOffset, a.totalRows, a.totalColumns) ==
          (b.rowIndex, b.columnIndex, b.rowOffset, b.columnOffset, b.totalRows, b.totalColumns),
          "Submatrix meta data has to be equal")

        SubmatrixBoolean(op(a.matrix, b.matrix), a.rowIndex, a.columnIndex, a.rowOffset, a.columnOffset, a.totalRows,
          a.totalColumns)
      }
    }
  }
}