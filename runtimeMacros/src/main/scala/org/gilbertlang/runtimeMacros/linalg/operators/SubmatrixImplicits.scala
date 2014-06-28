package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.linalg.Axis
import breeze.linalg.support._
import breeze.linalg.support.CanTraverseValues.ValuesVisitor
import org.gilbertlang.runtimeMacros.linalg.{Subvector, DoubleVector, DoubleMatrix, Submatrix}

trait SubmatrixImplicits extends DoubleMatrixImplicits {
  implicit def handholdSM: CanMapValues.HandHold[Submatrix, Double] = new CanMapValues.HandHold[Submatrix, Double]

  implicit def canZipMapValuesSM: CanZipMapValues[Submatrix, Double, Double, Submatrix] = {
    new CanZipMapValues[Submatrix, Double, Double, Submatrix]{
      def map(from1: Submatrix, from2: Submatrix, fn: (Double, Double) => Double): Submatrix = {
        require(from1.rowIndex == from2.rowIndex && from1.columnIndex == from2.columnIndex)
        require(from1.rows == from2.rows && from1.cols == from2.cols)

        val mapper = implicitly[CanZipMapValues[DoubleMatrix, Double, Double, DoubleMatrix]]
        import from1._
        Submatrix(mapper.map(from1.matrixValue, from2.matrixValue, fn), rowIndex, columnIndex, rowOffset, columnOffset, totalRows,
          totalColumns)
      }
    }
  }

  implicit def canSliceRowSM: CanSlice2[Submatrix, Int, ::.type, Submatrix] = {
    new CanSlice2[Submatrix, Int, ::.type, Submatrix]{
      def apply(submatrix: Submatrix, row: Int, ignored: ::.type ) = {
        import submatrix._
        Submatrix(matrixValue(row-rowOffset, ::), 0, columnIndex, 0, columnOffset, 1, totalColumns)
      }
    }
  }

  implicit def canSliceColSM: CanSlice2[Submatrix, ::.type, Int, Subvector] = {
    new CanSlice2[Submatrix, ::.type, Int, Subvector]{
      def apply(submatrix: Submatrix, ignored: ::.type, col: Int) = {
        import submatrix._
        Subvector(matrixValue(::, col-columnOffset), rowIndex, rowOffset, totalRows)
      }
    }
  }

  implicit def canTraverseValuesSM: CanTraverseValues[Submatrix, Double] = {
    new CanTraverseValues[Submatrix, Double] {
      def isTraversableAgain(from: Submatrix): Boolean = true

      def traverse(from: Submatrix, fn: ValuesVisitor[Double]): Unit = {
        val traverser = implicitly[CanTraverseValues[DoubleMatrix, Double]]
        traverser.traverse(from.matrixValue, fn)
      }
    }
  }

  implicit def canCollapseRowsSM: CanCollapseAxis[Submatrix, Axis._0.type, DoubleVector, Double, Submatrix] = {
    new CanCollapseAxis[Submatrix, Axis._0.type, DoubleVector, Double, Submatrix]{
      def apply(submatrix: Submatrix, axis: Axis._0.type)(fn: DoubleVector => Double) = {
        val collapser = implicitly[CanCollapseAxis[DoubleMatrix, Axis._0.type, DoubleVector, Double, DoubleMatrix]]
        import submatrix._
        Submatrix(collapser(submatrix.matrixValue, axis)(fn), 0, columnIndex, 0, columnOffset, 1,totalColumns)
      }
    }
  }

  implicit def canCollapseColsSM: CanCollapseAxis[Submatrix, Axis._1.type, DoubleVector, Double, Subvector] = {
    new CanCollapseAxis[Submatrix, Axis._1.type, DoubleVector, Double, Subvector]{
      def apply(submatrix: Submatrix, axis: Axis._1.type)(fn: DoubleVector => Double) = {
        val collapser = implicitly[CanCollapseAxis[DoubleMatrix, Axis._1.type, DoubleVector, Double, DoubleVector]]
        import submatrix._
        Subvector(collapser(submatrix.matrixValue, axis)(fn), rowIndex, rowOffset, totalRows)
      }
    }
  }

  implicit def handholdCanCollapseColsSM: CanCollapseAxis.HandHold[Submatrix, Axis._1.type,
    DoubleVector] = new CanCollapseAxis.HandHold[Submatrix, Axis._1.type, DoubleVector]()

  implicit def handholdCanCollapseRowsSM: CanCollapseAxis.HandHold[Submatrix, Axis._0.type,
    DoubleVector] = new CanCollapseAxis.HandHold[Submatrix, Axis._0.type, DoubleVector]()

}
