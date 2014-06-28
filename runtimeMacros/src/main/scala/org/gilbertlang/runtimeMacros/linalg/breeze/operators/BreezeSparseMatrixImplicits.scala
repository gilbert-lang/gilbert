package org.gilbertlang.runtimeMacros.linalg.breeze.operators

import breeze.linalg.support.{CanCollapseAxis, CanCopy, CanSlice2, CanZipMapValues}
import breeze.linalg.{Axis, CSCMatrix, SparseVector}
import breeze.math.Semiring
import breeze.storage.DefaultArrayValue

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Created by till on 01/04/14.
 */
trait BreezeSparseMatrixImplicits {
  implicit def canCopySparseMatrix[T: ClassTag]: CanCopy[CSCMatrix[T]] = new CanCopy[CSCMatrix[T]]{
    override def apply(matrix: CSCMatrix[T]): CSCMatrix[T] = matrix.copy.asInstanceOf[CSCMatrix[T]]
  }

  implicit def canSliceRowSparseMatrix[@specialized(Double, Boolean) T: ClassTag: Semiring: DefaultArrayValue]:
  CanSlice2[CSCMatrix[T],Int, ::.type, CSCMatrix[T]] = {
    new CanSlice2[CSCMatrix[T],Int, ::.type, CSCMatrix[T]] {
      override def apply(matrix: CSCMatrix[T], row: Int, ignored: ::.type) = {
        val builder = new CSCMatrix.Builder[T](1, matrix.cols, (matrix.activeSize.toDouble / matrix.rows).ceil.toInt)

        for (col <- 0 until matrix.cols) {
          var rowIndex = matrix.colPtrs(col)
          val endRowIndex = matrix.colPtrs(col + 1)

          while (rowIndex < endRowIndex && matrix.rowIndices(rowIndex) < row) {
            rowIndex += 1
          }

          if (rowIndex < endRowIndex && matrix.rowIndices(rowIndex) == row)
            builder.add(row, col, matrix.data(rowIndex))
        }

        builder.result
      }
    }
  }

  implicit def canSliceRowsSparseMatrix[@specialized(Double, Boolean) T: ClassTag: Semiring: DefaultArrayValue]:
  CanSlice2[CSCMatrix[T], Range, ::.type, CSCMatrix[T]] = {
    new CanSlice2[CSCMatrix[T],Range, ::.type, CSCMatrix[T]] {
      override def apply(matrix: CSCMatrix[T], rows: Range, ignored: ::.type) = {
        val builder = new CSCMatrix.Builder[T](rows.size, matrix.cols, rows.size *
          (matrix.activeSize.toDouble / matrix.rows).ceil.toInt)

        for(col <- 0 until matrix.cols) {
          var rowIndex = matrix.colPtrs(col)
          val endRowIndex = matrix.colPtrs(col + 1)

          while(rowIndex < endRowIndex && matrix.rowIndices(rowIndex) < rows.min){
            rowIndex += 1
          }

          while(rowIndex < endRowIndex && matrix.rowIndices(rowIndex) <= rows.max){
            if(rows.contains(matrix.rowIndices(rowIndex)))
              builder.add(matrix.rowIndices(rowIndex), col, matrix.data(rowIndex))

            rowIndex += 1
          }

        }

        builder.result
      }
    }
  }

  implicit def canSliceColSparseMatrix[@specialized(Double, Boolean) T: ClassTag: DefaultArrayValue]:
  CanSlice2[CSCMatrix[T], ::.type,Int, SparseVector[T]] = {
    new CanSlice2[CSCMatrix[T], ::.type,Int, SparseVector[T]] {
      override def apply(matrix: CSCMatrix[T], ignored: ::.type, col: Int): SparseVector[T]= {
        val rowStartIndex = matrix.colPtrs(col)
        val rowEndIndex = matrix.colPtrs(col + 1)
        val indices = new Array[Int](rowEndIndex - rowStartIndex)
        val data = new Array[T](rowEndIndex - rowStartIndex)
        var counter = 0

        while (counter < rowEndIndex - rowStartIndex) {
          data(counter) = matrix.data(rowStartIndex + counter)
          indices(counter) = matrix.rowIndices(rowStartIndex + counter)
          counter += 1
        }

        new SparseVector(indices, data, matrix.rows)
      }
    }
  }

  implicit def canSliceColsSparseMatrix[@specialized(Double, Boolean) T: ClassTag: Semiring: DefaultArrayValue]:
  CanSlice2[CSCMatrix[T], ::.type, Range, CSCMatrix[T]] = {
    new CanSlice2[CSCMatrix[T], ::.type, Range, CSCMatrix[T]] {
      override def apply(matrix: CSCMatrix[T], ignored: ::.type, cols: Range) = {
        var used: Int = 0

        for (col <- cols) {
          val rowStartIndex = matrix.colPtrs(col)
          val rowEndIndex = matrix.colPtrs(col + 1)
          used += rowEndIndex - rowStartIndex
        }

        val builder = new CSCMatrix.Builder[T](matrix.rows, cols.size, used)

        for (col <- cols) {
          var rowIndex = matrix.colPtrs(col)
          val rowEndIndex = matrix.colPtrs(col + 1)

          while (rowIndex < rowEndIndex) {
            builder.add(matrix.rowIndices(rowIndex), col, matrix.data(rowIndex))
            rowIndex += 1
          }
        }

        builder.result
      }
    }
  }

  implicit def canZipMapValuesSparseMatrix[@specialized(Double, Boolean) T: DefaultArrayValue: ClassTag: Semiring]: CanZipMapValues[CSCMatrix[T], T, T, CSCMatrix[T]] = {
    new CanZipMapValues[CSCMatrix[T], T, T, CSCMatrix[T]]{
      override def map(a: CSCMatrix[T], b: CSCMatrix[T], fn: (T, T) => T) = {
        val ring = implicitly[Semiring[T]]
        val zero = ring.zero
        val leaveOutZeros = fn(zero,zero) == zero
        val initSize = if (leaveOutZeros) a.activeSize + b.activeSize else a.rows* a.cols
        val builder = new CSCMatrix.Builder[T](a.rows, a.cols, initSize)

        if(!leaveOutZeros){
          val value = fn(zero,zero)

          for(r <- 0 until a.rows; c <- 0 until a.cols){
            builder.add(r, c, value)
          }
        }

        for(col <- 0 until a.cols){
          var aRow = a.colPtrs(col)
          val endARowIndex=  a.colPtrs(col+1)
          var bRow = b.colPtrs(col)
          val endBRowIndex = b.colPtrs(col+1)

          while(aRow < endARowIndex && bRow < endBRowIndex){
            val aRowIdx = a.rowIndices(aRow)
            val bRowIdx = b.rowIndices(bRow)

            if(aRowIdx == bRowIdx){
              builder.add(aRowIdx, col, fn(a.data(aRow), b.data(bRow)))
              aRow += 1
              bRow += 1
            }else{
              if(aRowIdx < bRowIdx){
                builder.add(aRowIdx, col, fn(a.data(aRow), zero))
                aRow += 1
              }else{
                builder.add(bRowIdx, col, fn(zero,b.data(bRow)))
                bRow += 1
              }
            }
          }

          while(aRow < endARowIndex){
            builder.add(a.rowIndices(aRow), col, fn(a.data(aRow),zero))
            aRow += 1
          }

          while(bRow < endBRowIndex){
            builder.add(b.rowIndices(bRow), col, fn(zero, b.data(bRow)))
            bRow += 1
          }
        }

        builder.result
      }
    }
  }

  implicit def canCollapseRowsSparseMatrix[@specialized(Double, Boolean) T: ClassTag: DefaultArrayValue: Semiring]:
  CanCollapseAxis[CSCMatrix[T], Axis._0.type, SparseVector[T],
    T, CSCMatrix[T]] = new CanCollapseAxis[CSCMatrix[T], Axis._0.type, SparseVector[T], T,
    CSCMatrix[T]]{
    override def apply(matrix: CSCMatrix[T], axis: Axis._0.type)(fn: SparseVector[T] => T) = {
      val builder = new CSCMatrix.Builder[T](1,matrix.cols, matrix.cols)

      for(col <- 0 until matrix.cols){
        builder.add(0,col, fn(matrix(::,col)))
      }

      builder.result
    }
  }

  implicit def canCollapseColsSparseMatrix[@specialized(Double, Boolean) T: ClassTag: DefaultArrayValue: Semiring]:
  CanCollapseAxis[CSCMatrix[T], Axis._1.type, SparseVector[T], T, SparseVector[T]] =
    new CanCollapseAxis[CSCMatrix[T], Axis._1.type, SparseVector[T], T, SparseVector[T]]{
      override def apply(matrix: CSCMatrix[T], axis: Axis._1.type)(fn: SparseVector[T] => T) = {
        val dataBuilder = mutable.ArrayBuilder.make[T]
        val indexBuilder = new mutable.ArrayBuilder.ofInt

        val t = matrix.t

        for(col <- 0 until t.cols){
          val value = fn(t(::, col))

          if(value != 0){
            dataBuilder += value
            indexBuilder += col
          }
        }
        val index = indexBuilder.result()
        val data = dataBuilder.result()
        new SparseVector(index, data, index.length, t.cols)
      }
    }
}
