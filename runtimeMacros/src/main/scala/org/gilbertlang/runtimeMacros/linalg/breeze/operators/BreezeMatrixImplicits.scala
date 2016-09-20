package org.gilbertlang.runtimeMacros.linalg.breeze.operators

import breeze.linalg._
import breeze.linalg.support.CanTraverseValues.ValuesVisitor
import breeze.linalg.support._
import breeze.math.Semiring
import breeze.storage.Zero

import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag

/**
 * Created by till on 01/04/14.
 */
trait BreezeMatrixImplicits extends BreezeSparseMatrixImplicits {
  implicit def canZipMapValuesMatrix[@specialized(Double, Boolean) T: ClassTag: Semiring]: CanZipMapValues[Matrix[T], T, T, Matrix[T]] = {
    new CanZipMapValues[Matrix[T], T, T, Matrix[T]]{
      override def map(a: Matrix[T], b:Matrix[T], fn: (T, T) => T) = {
        (a,b) match {
          case (x: DenseMatrix[T], y: DenseMatrix[T]) =>
            val zipMapValues = implicitly[CanZipMapValues[DenseMatrix[T], T, T, DenseMatrix[T]]]
            zipMapValues.map(x,y, fn)
          case (x: CSCMatrix[T], y: CSCMatrix[T]) =>
            val zipMapValues = implicitly[CanZipMapValues[CSCMatrix[T], T, T, CSCMatrix[T]]]
            zipMapValues.map(x,y,fn)
          case (x,y) =>
            val result = DenseMatrix.zeros[T](x.rows, y.cols)
            (x.iterator zip y.iterator) foreach { case (((row, col), valueX),(_, valueY)) => result.update(row, col,
              fn(valueX, valueY))}
            result
        }
      }
    }
  }

  implicit def canMapMatrixValues[T, R: ClassTag :Semiring]: support.CanMapValues[Matrix[T], T, R, Matrix[R]] = {
    new CanMapValues[Matrix[T], T, R, Matrix[R]]{
      /**Maps all key-value pairs from the given collection. */
      override def apply(from: Matrix[T], fn: (T => R)): Matrix[R] = {
        from match{
          case x: DenseMatrix[T] => x.map(fn)(DenseMatrix.canMapValues)
          case x: CSCMatrix[T] => x.map(fn)(CSCMatrix.canMapValues)
          case x =>
            val result = DenseMatrix.zeros[R](x.rows, x.cols)
            x.iterator foreach { case ((row, col), value) => result.update(row, col, fn(value))}
            result
        }
      }
    }
  }

  implicit def canMapActiveMatrixValues[T, R: ClassTag: Semiring]: support.CanMapActiveValues[Matrix[T], T, R, Matrix[R]] = {
    new CanMapActiveValues[Matrix[T], T, R, Matrix[R]] {
      /**Maps all active key-value pairs from the given collection. */
      override def apply(from: Matrix[T], fn: (T) => R): Matrix[R] = {
        from match {
          case x: DenseMatrix[T] => x.mapValues(fn)(DenseMatrix.canMapValues)
          case x: CSCMatrix[T] => x.mapActiveValues(fn)(CSCMatrix.canMapActiveValues)
          case x =>
            val result = DenseMatrix.zeros[R](x.rows, x.cols)
            x.activeIterator foreach { case ((row, col), value) => result.update(row, col, fn(value)) }
            result
        }
      }
    }
  }

  implicit def canTransposeMatrixBM[@specialized(Double, Boolean) T:ClassTag :Semiring]:
  CanTranspose[Matrix[T], Matrix[T]] = {
    new CanTranspose[Matrix[T], Matrix[T]]{
      override def apply(from: Matrix[T]):Matrix[T] = {
        from match {
          case x: DenseMatrix[T] => x.t
          case x: CSCMatrix[T] => x.t
          case x =>
            val result = DenseMatrix.zeros[T](x.cols, x.rows)
            x.activeIterator foreach { case ((row, col), value) => result.update(col, row, value)}
            result
        }
      }
    }
  }

  implicit def canTraverseValuesBM[@specialized(Double, Boolean) T]:CanTraverseValues[Matrix[T], T] = {
    new CanTraverseValues[Matrix[T], T] {
      override def isTraversableAgain(matrix: Matrix[T]):Boolean = true

      override def traverse(matrix: Matrix[T], fn: ValuesVisitor[T]){
        matrix match {
          case x: DenseMatrix[T] =>
            val traversal = implicitly[CanTraverseValues[DenseMatrix[T], T]]
            traversal.traverse(x, fn)
          case x: CSCMatrix[T] =>
            val traversal = implicitly[CanTraverseValues[CSCMatrix[T], T]]
            traversal.traverse(x, fn)
          case x =>
            for(row <- 0 until x.rows; col <- 0 until x.cols){
              fn.visit(x(row,col))
            }
        }
      }
    }
  }

  implicit def canSliceRowMatrix[T: ClassTag: Semiring]: CanSlice2[Matrix[T], Int, ::.type,
    Matrix[T]] = {
    new CanSlice2[Matrix[T], Int, ::.type, Matrix[T]]{
      override def apply(matrix: Matrix[T], row: Int, ignored: ::.type) = {
        matrix match {
          case x: DenseMatrix[T] => x(row, ::)(DenseMatrix.canSliceRow).inner.asDenseMatrix
          case x: CSCMatrix[T] => x(row, ::)(canSliceRowSparseMatrix)
          case x =>
            val result = DenseMatrix.zeros[T](1, x.cols)
            for(col <- 0 until x.cols){
              result.update(0, col, x(row,col))
            }
            result
        }
      }
    }
  }

  implicit def canSliceRowsMatrix[T: ClassTag: Semiring]: CanSlice2[Matrix[T], Range, ::.type, Matrix[T]] = {
    new CanSlice2[Matrix[T], Range, ::.type, Matrix[T]]{
      override def apply(matrix: Matrix[T], rows: Range, ignored: ::.type) = {
        matrix match {
          case x: DenseMatrix[T] => x(rows, ::)(DenseMatrix.canSliceRows)
          case x: CSCMatrix[T] => x(rows, ::)(canSliceRowsSparseMatrix)
          case x =>
            val result = DenseMatrix.zeros[T](rows.length, x.cols)
            for((row,idx) <- (rows zipWithIndex); col <- 0 until x.cols){
              result.update(idx, col, x(row, col))
            }
            result
        }
      }
    }
  }

  implicit def canSliceColMatrix[T: ClassTag: Semiring]: CanSlice2[Matrix[T], ::.type, Int, Vector[T]] = {
    new CanSlice2[Matrix[T], ::.type, Int, Vector[T]] {
      override def apply(matrix: Matrix[T], ignored: ::.type, col:Int) = {
        matrix match {
          case x: DenseMatrix[T] => x(::, col)(DenseMatrix.canSliceCol)
          case x: CSCMatrix[T] => x(::, col)(canSliceColSparseMatrix)
          case x =>
            val result = DenseVector.zeros[T](x.rows)
            for(row <- 0 until x.rows){
              result.update(row, x(row,col))
            }
            result
        }
      }
    }
  }

  implicit def canSliceColsMatrix[T: ClassTag: Semiring]: CanSlice2[Matrix[T], ::.type, Range, Matrix[T]] = {
    new CanSlice2[Matrix[T], ::.type, Range, Matrix[T]] {
      override def apply(matrix: Matrix[T], ignored: ::.type, cols: Range) = {
        matrix match {
          case x: DenseMatrix[T] => x(::, cols)(DenseMatrix.canSliceCols)
          case x: CSCMatrix[T] => x(::, cols)(canSliceColsSparseMatrix)
          case x =>
            val result = DenseMatrix.zeros[T](x.rows, cols.length)
            for(row <- 0 until x.rows; (col,idx) <- (cols zipWithIndex)){
              result.update(row, idx, x(row, col))
            }
            result
        }
      }
    }
  }

  implicit def canCollapseRowsMatrix[T: ClassTag: Semiring: Zero]: CanCollapseAxis[Matrix[T], Axis._0.type, Vector[T], T, Matrix[T]] = {
    new CanCollapseAxis[Matrix[T], Axis._0.type, Vector[T], T, Matrix[T]]{
      override def apply(matrix: Matrix[T], axis: Axis._0.type)(fn: Vector[T] => T) = {
        var result: DenseMatrix[T] = null

        for (c <- 0 until matrix.cols) {
          val col = fn(matrix(::, c))

          if (result eq null) {
            result = DenseMatrix.zeros(1, matrix.cols)
          }

          result(0, c) = col
        }

        if (result eq null) {
          DenseMatrix.zeros[T](0, matrix.cols)
        } else {
          result
        }
      }
    }
  }

  implicit def canCollapseColsMatrix[T: ClassTag: Semiring]: CanCollapseAxis[Matrix[T], Axis._1.type, Vector[T], T,
    Vector[T]] = new CanCollapseAxis[Matrix[T], Axis._1.type, Vector[T], T, Vector[T]]{
    override def apply(matrix: Matrix[T], axis: Axis._1.type)(fn: Vector[T] => T) = {
      matrix match {
        case x: DenseMatrix[T] =>
          val collapser = implicitly[CanCollapseAxis[DenseMatrix[T], Axis._1.type, DenseVector[T], T, DenseVector[T]]]
          collapser(x, axis)(fn)
        case x: CSCMatrix[T] =>
          val collapser = implicitly[CanCollapseAxis[CSCMatrix[T], Axis._1.type, SparseVector[T], T, SparseVector[T]]]
          collapser(x, axis)(fn)
        case x =>
          val t = x.t
          val result = DenseVector.zeros[T](t.cols)
          for(col <- 0 until t.cols){
            result.update(col, fn(t(::,col)))
          }
          result
      }
    }
  }

  implicit def handholdCanMapColsMatrix[T: ClassTag: Semiring]: CanCollapseAxis
  .HandHold[Matrix[T], Axis._1.type, Vector[T]] =
    new CanCollapseAxis.HandHold[Matrix[T], Axis._1.type, Vector[T]]()

  implicit def handholdCanMapRowsMatrix[T: ClassTag: Semiring]: CanCollapseAxis
  .HandHold[Matrix[T], Axis._0.type, Vector[T]] =
    new CanCollapseAxis.HandHold[Matrix[T], Axis._0.type, Vector[T]]()
}
