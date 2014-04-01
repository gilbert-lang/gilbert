package org.gilbertlang.runtimeMacros.linalg.operators

import scala.reflect.ClassTag
import breeze.linalg.support._
import breeze.linalg._
import breeze.storage.DefaultArrayValue
import breeze.math.Semiring
import breeze.linalg.support.CanTraverseValues.ValuesVisitor
import scala.language.implicitConversions

/**
 * Created by till on 01/04/14.
 */
trait BreezeMatrixImplicits extends BreezeSparseMatrixImplicits {
  implicit def canZipMapValuesMatrix[@specialized(Double, Boolean) T: DefaultArrayValue: ClassTag: Semiring]: CanZipMapValues[Matrix[T], T, T, Matrix[T]] = {
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
            val data = ((x.valuesIterator zip (y.valuesIterator)) map { case (x,y) => fn(x,y) }).toArray[T]
            new DenseMatrix[T](x.rows, y.cols, data)
        }
      }
    }
  }

  implicit def canMapMatrixValues[@specialized(Double, Boolean) T: ClassTag:DefaultArrayValue:Semiring]: support
  .CanMapValues[Matrix[T], T, T,
    Matrix[T]] = {
    new CanMapValues[Matrix[T], T, T, Matrix[T]]{
      /**Maps all key-value pairs from the given collection. */
      def map(from: Matrix[T], fn: (T => T)): Matrix[T] = {
        from match{
          case x: DenseMatrix[T] => x.map(fn)
          case x: CSCMatrix[T] => x.map(fn)
          case x =>
            val data = (x.valuesIterator map { value => fn(value) }).toArray[T]
            new DenseMatrix[T](x.rows, x.cols, data)
        }
      }

      /**Maps all active key-value pairs from the given collection. */
      def mapActive(from: Matrix[T], fn: (T => T)): Matrix[T] = {
        from match{
          case x: DenseMatrix[T] => x.map(fn)
          case x: CSCMatrix[T] => x.map(fn)
          case x =>
            val data = (x.valuesIterator map { value => fn(value) }).toArray[T]
            new DenseMatrix[T](x.rows, x.cols, data)
        }
      }
    }
  }

  implicit def canTransposeMatrix[@specialized(Double, Boolean) T:ClassTag:DefaultArrayValue:Semiring]: CanTranspose[Matrix[T], Matrix[T]] = {
    new CanTranspose[Matrix[T], Matrix[T]]{
      override def apply(from: Matrix[T]):Matrix[T] = {
        from match {
          case x: DenseMatrix[T] => x.t
          case x: CSCMatrix[T] => x.t
          case x =>
            val data = (x.valuesIterator).toArray[T]
            val result = new DenseMatrix[T](x.rows, x.cols, data)
            result.t
        }
      }
    }
  }

  implicit def canTraverseValues[T]:CanTraverseValues[Matrix[T], T] = {
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

  implicit def canSliceRowMatrix[T: ClassTag: Semiring: DefaultArrayValue]: CanSlice2[Matrix[T], Int, ::.type, Matrix[T]] = {
    new CanSlice2[Matrix[T], Int, ::.type, Matrix[T]]{
      override def apply(matrix: Matrix[T], row: Int, ignored: ::.type) = {
        matrix match {
          case x: DenseMatrix[T] => x(row, ::)
          case x: CSCMatrix[T] => x(row, ::)
          case x =>
            val data = (for(col <- 0 until x.cols) yield x(row,col)).toArray[T]
            new DenseMatrix[T](1,x.cols, data);
        }
      }
    }
  }

  implicit def canSliceRowsMatrix[T: ClassTag: Semiring: DefaultArrayValue]: CanSlice2[Matrix[T], Range, ::.type, Matrix[T]] = {
    new CanSlice2[Matrix[T], Range, ::.type, Matrix[T]]{
      override def apply(matrix: Matrix[T], rows: Range, ignored: ::.type) = {
        matrix match {
          case x: DenseMatrix[T] => x(rows, ::)
          case x: CSCMatrix[T] => x(rows, ::)
          case x =>
            val data = (for(row <- rows; col <- 0 until x.cols) yield x(row,col)).toArray[T]
            new DenseMatrix[T](rows.length, x.cols, data)
        }
      }
    }
  }

  implicit def canSliceColMatrix[T: ClassTag: Semiring: DefaultArrayValue]: CanSlice2[Matrix[T], ::.type, Int, Vector[T]] = {
    new CanSlice2[Matrix[T], ::.type, Int, Vector[T]] {
      override def apply(matrix: Matrix[T], ignored: ::.type, col:Int) = {
        matrix match {
          case x: DenseMatrix[T] => x(::, col)
          case x: CSCMatrix[T] => x(::, col)
          case x =>
            val data = (for(row <- 0 until x.rows) yield x(row, col)).toArray[T]
            new DenseVector[T](data)
        }
      }
    }
  }

  implicit def canSliceColsMatrix[T: ClassTag: Semiring: DefaultArrayValue]: CanSlice2[Matrix[T], ::.type, Range, Matrix[T]] = {
    new CanSlice2[Matrix[T], ::.type, Range, Matrix[T]] {
      override def apply(matrix: Matrix[T], ignored: ::.type, cols: Range) = {
        matrix match {
          case x: DenseMatrix[T] => x(::, cols)
          case x: CSCMatrix[T] => x(::, cols)
          case x =>
            val data = (for(row <- 0 until x.rows; col <- cols) yield x(row, col)).toArray[T]
            new DenseMatrix(x.rows, cols.length, data)
        }
      }
    }
  }

  implicit def canCollapseRowsMatrix[T: ClassTag: DefaultArrayValue: Semiring]: CanCollapseAxis[Matrix[T], Axis._0.type, Vector[T], T, Matrix[T]] = {
    new CanCollapseAxis[Matrix[T], Axis._0.type, Vector[T], T, Matrix[T]]{
      override def apply(matrix: Matrix[T], axis: Axis._0.type)(fn: Vector[T] => T) = {
        matrix match {
          case x: DenseMatrix[T] =>
            val collapser = implicitly[CanCollapseAxis[DenseMatrix[T], Axis._0.type, DenseVector[T], T,
              DenseMatrix[T]]]
            collapser(x, axis)(fn)
          case x: CSCMatrix[T] =>
            val collapser = implicitly[CanCollapseAxis[CSCMatrix[T], Axis._0.type, SparseVector[T], T, CSCMatrix[T]]]
            collapser(x, axis)(fn)
          case x =>
            val data = new Array[T](x.cols)

            for(col <- 0 until x.cols){
              data(col) = fn(x(::, col))
            }
            new DenseMatrix(1, x.cols, data)
        }
      }
    }
  }

  implicit def canCollapseColsMatrix[T: ClassTag: DefaultArrayValue: Semiring]: CanCollapseAxis[Matrix[T], Axis._1.type, Vector[T], T,
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
          val data = new Array[T](x.rows)
          val t = x.t

          for(col <- 0 until t.cols){
            data(col) = fn(t(::, col))
          }
          new DenseVector(data)
      }
    }
  }

  implicit def handholdCanMapColsMatrix[T: ClassTag: DefaultArrayValue: Semiring]: CanCollapseAxis
  .HandHold[Matrix[T], Axis._1.type, Vector[T]] =
    new CanCollapseAxis.HandHold[Matrix[T], Axis._1.type, Vector[T]]()

  implicit def handholdCanMapRowsMatrix[T: ClassTag: DefaultArrayValue: Semiring]: CanCollapseAxis
  .HandHold[Matrix[T], Axis._0.type, Vector[T]] =
    new CanCollapseAxis.HandHold[Matrix[T], Axis._0.type, Vector[T]]()


  class VectorDecorator[T:ClassTag:Semiring:DefaultArrayValue](vector: Vector[T]) extends BreezeVectorImplicits{

    def asMatrix: Matrix[T] = {
      vector match {
        case x: DenseVector[T] => x.asDenseMatrix
        case x: SparseVector[T] => x.asSparseMatrix
        case x =>
          val data = vector.valuesIterator.toArray[T]
          new DenseMatrix(vector.length, 1, data)
      }
    }
  }

  implicit def vectorDecorator[T:ClassTag:Semiring:DefaultArrayValue](vector: Vector[T]):VectorDecorator[T] = new VectorDecorator(vector)
}
