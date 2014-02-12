package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.linalg.support.CanSlice2
import breeze.linalg.Matrix
import breeze.linalg.CSCMatrix
import breeze.linalg.SparseVector
import scala.reflect.ClassTag
import breeze.storage.DefaultArrayValue
import breeze.math.Semiring
import scala.collection.immutable.{:: => ::}
import breeze.linalg.support.CanZipMapValues
import breeze.linalg.DenseMatrix
import breeze.linalg.support.CanCollapseAxis
import breeze.linalg.Axis
import scala.collection.mutable.ArrayBuilder
import breeze.linalg.norm

trait BreezeMatrixOps {

  implicit def canSliceRowSparseMatrix[T: ClassTag: Semiring: DefaultArrayValue]: 
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

  implicit def canSliceRowsSparseMatrix[T: ClassTag: Semiring: DefaultArrayValue]: 
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

  implicit def canSliceColSparseMatrix[T: ClassTag: DefaultArrayValue]: 
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

  implicit def canSliceColsSparseMatrix[T: ClassTag: Semiring: DefaultArrayValue]: 
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
  
  implicit val canZipMapValuesSparseMatrix: CanZipMapValues[CSCMatrix[Double], Double, Double, CSCMatrix[Double]] = {
    new CanZipMapValues[CSCMatrix[Double], Double, Double, CSCMatrix[Double]]{
      override def map(a: CSCMatrix[Double], b: CSCMatrix[Double], fn: (Double, Double) => Double) = {
        val leaveOutZeros = fn(0,0) == 0
        val initSize = if (leaveOutZeros) a.activeSize + b.activeSize else a.rows* a.cols
        val builder = new CSCMatrix.Builder[Double](a.rows, a.cols, initSize)
        
        if(!leaveOutZeros){
          val value = fn(0,0)
          
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
                builder.add(aRowIdx, col, fn(a.data(aRow), 0))
                aRow += 1
              }else{
                builder.add(bRowIdx, col, fn(0,b.data(bRow)))
                bRow += 1
              }
            }
          }
          
          while(aRow < endARowIndex){
            builder.add(a.rowIndices(aRow), col, fn(a.data(aRow),0))
            aRow += 1
          }
          
          while(bRow < endBRowIndex){
            builder.add(b.rowIndices(bRow), col, fn(0, b.data(bRow)))
            bRow += 1
          }
        }
        
        builder.result
      }
    }
  }
  
  implicit val canZipMapValuesMatrix: CanZipMapValues[Matrix[Double], Double, Double, Matrix[Double]] = {
    new CanZipMapValues[Matrix[Double], Double, Double, Matrix[Double]]{
      override def map(a: Matrix[Double], b:Matrix[Double], fn: (Double, Double) => Double) = {
        val data = ((a.valuesIterator zip (b.valuesIterator)) map { case (a,b) => fn(a,b) }).toArray[Double]
        new DenseMatrix[Double](a.rows, a.cols, data)
      }
      
    }
  }
  
  implicit val canCollapseRowsSparseMatrix: CanCollapseAxis[CSCMatrix[Double], Axis._0.type, SparseVector[Double], 
    Double, CSCMatrix[Double]] = new CanCollapseAxis[CSCMatrix[Double], Axis._0.type, SparseVector[Double], Double,
      CSCMatrix[Double]]{
    override def apply(matrix: CSCMatrix[Double], axis: Axis._0.type)(fn: SparseVector[Double] => Double) = {
      val builder = new CSCMatrix.Builder[Double](1,matrix.cols, matrix.cols)
      
      for(col <- 0 until matrix.cols){
        builder.add(0,col, fn(matrix(::,col)))
      }
      
      builder.result
    }
  }
  
  implicit val canCollapseColsSparseMatrix: CanCollapseAxis[CSCMatrix[Double], Axis._1.type, SparseVector[Double], 
    Double, SparseVector[Double]] = new CanCollapseAxis[CSCMatrix[Double], Axis._1.type, SparseVector[Double], Double,
      SparseVector[Double]]{
    override def apply(matrix: CSCMatrix[Double], axis: Axis._1.type)(fn: SparseVector[Double] => Double) = {
      val dataBuilder = new ArrayBuilder.ofDouble
      val indexBuilder = new ArrayBuilder.ofInt
      
      val t = matrix.t
      
      for(col <- 0 until t.cols){
        val value = fn(t(::, col))
        
        if(value != 0){
          dataBuilder += value
          indexBuilder += col
        }
      }
      val index = indexBuilder.result
      val data = dataBuilder.result
      new SparseVector(index, data, index.length, t.cols)
    }
  }
  
  val dense = DenseMatrix((1,2),(2,3))
  val slices2 = dense(::, breeze.linalg.*)
  val slices = dense(breeze.linalg.*, ::)
  val result = norm(slices)
}