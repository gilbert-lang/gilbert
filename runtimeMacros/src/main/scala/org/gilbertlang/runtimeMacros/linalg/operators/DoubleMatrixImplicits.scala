package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.linalg.{Vector, Axis, Matrix}
import org.apache.mahout.math.{Matrix => MahoutMatrix, Vector => MahoutVector}
import breeze.linalg.support.CanTraverseValues.ValuesVisitor
import breeze.linalg.support._
import org.gilbertlang.runtimeMacros.linalg.mahout.operators.MahoutMatrixImplicits
import org.gilbertlang.runtimeMacros.linalg.{DoubleVector, DoubleMatrix}
import org.gilbertlang.runtimeMacros.linalg.breeze._
import org.gilbertlang.runtimeMacros.linalg.mahout._
import org.gilbertlang.runtimeMacros.linalg.breeze.operators.BreezeMatrixImplicits


trait DoubleMatrixImplicits extends BreezeMatrixImplicits with MahoutMatrixImplicits {

  implicit def handholdDM: CanMapValues.HandHold[DoubleMatrix, Double] = new CanMapValues.HandHold[DoubleMatrix,
    Double]

  implicit def canZipMapValuesDM: CanZipMapValues[DoubleMatrix, Double, Double,
    DoubleMatrix] = new CanZipMapValues[DoubleMatrix, Double, Double, DoubleMatrix]{
    def map(from1: DoubleMatrix, from2: DoubleMatrix, fn: (Double, Double) => Double): DoubleMatrix = {
      (from1, from2) match {
        case (x: BreezeDoubleMatrix, y: BreezeDoubleMatrix) =>
          val zipper = implicitly[CanZipMapValues[Matrix[Double], Double, Double, Matrix[Double]]]
          zipper.map(x.matrix, y.matrix, fn)
        case (x: MahoutDoubleMatrix, y: MahoutDoubleMatrix) =>
          val result = x.matrix.copy.assign(y.matrix, fn)
          MahoutDoubleMatrix(result)
        case _ => throw new IllegalArgumentException("Does not support CanZipMapValues for " + from1.getClass + ", " +
          "" + from2.getClass)
      }
    }
  }

  implicit def canTraverseValuesDM: CanTraverseValues[DoubleMatrix, Double] = {
    new CanTraverseValues[DoubleMatrix, Double] {
      def isTraversableAgain(from: DoubleMatrix): Boolean = true

      def traverse(from: DoubleMatrix, fn: ValuesVisitor[Double]): Unit = {
        from match {
          case x:BreezeDoubleMatrix =>
            val traverser = implicitly[CanTraverseValues[Matrix[Double], Double]]
            traverser.traverse(x.matrix, fn)
          case x:MahoutDoubleMatrix =>
            val traverser = implicitly[CanTraverseValues[MahoutMatrix, Double]]
            traverser.traverse(x.matrix, fn)
          case _ => throw new IllegalArgumentException("Does not support canTraverseValues for " + from.getClass)
        }
      }
    }
  }

  implicit def canCollapseRowsDM: CanCollapseAxis[DoubleMatrix, Axis._0.type, DoubleVector, Double,
    DoubleMatrix] = new CanCollapseAxis[DoubleMatrix, Axis._0.type, DoubleVector, Double, DoubleMatrix]{
    override def apply(matrix: DoubleMatrix, axis: Axis._0.type)(fn: DoubleVector => Double) = {
      matrix match {
        case m: BreezeDoubleMatrix =>
          val collapser = implicitly[CanCollapseAxis[Matrix[Double], Axis._0.type, Vector[Double], Double,
            Matrix[Double]]]
          val f = (x: Vector[Double]) => fn(new BreezeDoubleVector(x))
          collapser(m.matrix, axis)(f)
        case m: MahoutDoubleMatrix =>
          val collapser = implicitly[CanCollapseAxis[MahoutMatrix, Axis._0.type, MahoutVector, Double,
            MahoutDoubleMatrix]]
          val f = (x: MahoutVector) => fn(new MahoutDoubleVector(x))
          collapser(m.matrix, axis)(f)
        case _ =>
          throw new IllegalArgumentException("Does not support canCollapseRowsDM for " + matrix.getClass)
      }

    }
  }

  implicit def canCollapseColsDM: CanCollapseAxis[DoubleMatrix, Axis._1.type, DoubleVector, Double,
    DoubleVector] = new CanCollapseAxis[DoubleMatrix, Axis._1.type, DoubleVector, Double, DoubleVector]{
    override def apply(matrix: DoubleMatrix, axis: Axis._1.type)(fn: DoubleVector => Double) = {
      matrix match {
        case m: BreezeDoubleMatrix =>
          val collapser = implicitly[CanCollapseAxis[Matrix[Double], Axis._1.type, Vector[Double], Double,
            Vector[Double]]]
          val f = (x: Vector[Double]) => fn(new BreezeDoubleVector(x))
          collapser(m.matrix, axis)(f)
        case m: MahoutDoubleMatrix =>
          val collapser = implicitly[CanCollapseAxis[MahoutMatrix, Axis._1.type, MahoutVector, Double,
            MahoutDoubleVector]]
          val f = (x: MahoutVector) => fn(new MahoutDoubleVector(x))
          collapser(m.matrix, axis)(f)
        case _ =>
          throw new IllegalArgumentException("Does not support canCollapseColsDM for " + matrix.getClass)
      }
    }
  }

  implicit def handholdCanCollapseColsDM: CanCollapseAxis
  .HandHold[DoubleMatrix, Axis._1.type, DoubleVector] =
    new CanCollapseAxis.HandHold[DoubleMatrix, Axis._1.type, DoubleVector]()

  implicit def handholdCanCollapseRowsDM: CanCollapseAxis
  .HandHold[DoubleMatrix, Axis._0.type, DoubleVector] =
    new CanCollapseAxis.HandHold[DoubleMatrix, Axis._0.type, DoubleVector]()

  implicit def canSliceRowDM: CanSlice2[DoubleMatrix, Int, ::.type, DoubleMatrix] = {
    new CanSlice2[DoubleMatrix, Int, ::.type, DoubleMatrix]{
      override def apply(matrix: DoubleMatrix, row: Int, ignored: ::.type) = {
        matrix match {
          case m: BreezeDoubleMatrix =>
            m.matrix(row, ::)
          case m: MahoutDoubleMatrix =>
            MahoutDoubleMatrix(m.matrix.viewPart(row, 1, 0, m.cols))
          case _ =>
            throw new IllegalArgumentException("Does not support canSliceRowDM for " + matrix.getClass)
        }
      }
    }
  }

  implicit def canSliceRowsDM: CanSlice2[DoubleMatrix, Range, ::.type, DoubleMatrix] = {
    new CanSlice2[DoubleMatrix, Range, ::.type, DoubleMatrix]{
      override def apply(matrix: DoubleMatrix, rows: Range, ignored: ::.type) = {
        matrix match {
          case m: BreezeDoubleMatrix =>
            m.matrix(rows, ::)
          case m: MahoutDoubleMatrix =>
            require(rows.step == 1)
            MahoutDoubleMatrix(m.matrix.viewPart(rows.start, rows.length, 0, m.cols))
          case _ =>
            throw new IllegalArgumentException("Does not support canSliceRowsDM for " + matrix.getClass)
        }
      }
    }
  }

  implicit def canSliceColDM: CanSlice2[DoubleMatrix, ::.type, Int, DoubleVector] = {
    new CanSlice2[DoubleMatrix, ::.type, Int, DoubleVector]{
      override def apply(matrix: DoubleMatrix, ignored: ::.type, col: Int) = {
        matrix match {
          case m: BreezeDoubleMatrix =>
            m.matrix(::, col)
          case m: MahoutDoubleMatrix =>
            MahoutDoubleVector(m.matrix.viewColumn(col))
          case _ =>
            throw new IllegalArgumentException("Does not support canSliceColDM for " + matrix.getClass)
        }
      }

    }
  }

  implicit def canSliceColsDM: CanSlice2[DoubleMatrix, ::.type, Range, DoubleMatrix] ={
    new CanSlice2[DoubleMatrix, ::.type, Range, DoubleMatrix]{
      override def apply(matrix: DoubleMatrix, ignored: ::.type, cols: Range) = {
        matrix match {
          case m: BreezeDoubleMatrix =>
            m.matrix(::, cols)
          case m: MahoutDoubleMatrix =>
            require(cols.step == 1)
            MahoutDoubleMatrix(m.matrix.viewPart(0, m.rows, cols.start, cols.length))
          case _ =>
            throw new IllegalArgumentException("Does not support canSliceColsDM for " + matrix.getClass)
        }
      }
    }
  }
}
