package org.gilbertlang.runtimeMacros.linalg

import breeze.linalg.Matrix
import breeze.linalg.{DenseMatrix => BreezeDenseMatrix, CSCMatrix => BreezeSparseMatrix }

trait MatrixFactory[@specialized T] {
  def create(rows: Int, cols: Int, dense: Boolean): Matrix[T]
  def init(rows: Int, cols: Int, initialValue: T, dense: Boolean): Matrix[T]
  def create(rows: Int, cols: Int, entries: Seq[(Int, Int, T)], dense: Boolean): Matrix[T]
  def eye(rows: Int, cols: Int, dense: Boolean): Matrix[T]
  def eye(rows: Int, cols: Int, startRow: Int, startCol: Int, dense: Boolean): Matrix[T]
}

object MatrixFactory{
  implicit object DoubleFactory extends MatrixFactory[Double] {
    def create(rows: Int, cols: Int, dense: Boolean): Matrix[Double] = {
      if(dense){
        BreezeDenseMatrix.zeros[Double](rows, cols)
      }else{
        BreezeSparseMatrix.zeros[Double](rows, cols)
      }
    }
    
    def init(rows: Int, cols: Int, initialValue: Double, dense: Boolean): Matrix[Double] = {
      val data = Array.fill[Double](rows*cols)(initialValue)
      if(dense){
        BreezeDenseMatrix.create[Double](rows, cols, data)
      }else{
        BreezeSparseMatrix.create[Double](rows, cols, data)
      }
    }
    
    def create(rows: Int, cols: Int, entries: Seq[(Int,Int,Double)], dense: Boolean): Matrix[Double] = {
      if(dense){
        val result = BreezeDenseMatrix.zeros[Double](rows, cols)
        for((row, col, value) <- entries){
          result.update(row, col, value)
        }
        result
      }else{
        val builder = new BreezeSparseMatrix.Builder[Double](rows, cols, entries.size)
        for((row, col, value) <- entries) {
          builder.add(row, col, value)
        }
        builder.result
      }
    }

    def eye(rows: Int, cols: Int, dense: Boolean): Matrix[Double] = {
      if (dense) {
        val result = BreezeDenseMatrix.zeros[Double](rows, cols)
        for (idx <- 0 until math.min(rows, cols)) {
          result.update(idx, idx, 1)
        }
        result
      }else{
        val builder = new BreezeSparseMatrix.Builder[Double](rows, cols, math.min(rows, cols))
        
        for(idx <- 0 until math.min(rows, cols)){
          builder.add(idx,idx,1)
        }
        
        builder.result
      }
    }

    def eye(rows: Int, cols: Int, startRow: Int, startCol: Int, dense: Boolean): Matrix[Double] = {
      if(dense){
        val result = BreezeDenseMatrix.zeros[Double](rows, cols)
        for(idx <- 0 until math.min(rows -startRow, cols-startCol)){
          result.update(idx+startRow, idx+startCol, 1.0)
        }

        result
      }else{
        val builder = new BreezeSparseMatrix.Builder[Double](rows, cols, math.min(rows-startRow, cols - startCol))

        for(idx <- 0 until math.min(rows-startRow, cols-startCol)){
          builder.add(idx+startRow, idx+startCol,1)
        }

        builder.result
      }
    }
  }


  implicit object BooleanFactory extends MatrixFactory[Boolean] {
    def create(rows: Int, cols: Int, dense: Boolean): Bitmatrix = {
      Bitmatrix.zeros(rows, cols)
    }
    
    def init(rows: Int, cols: Int, initialValue: Boolean, dense: Boolean): Bitmatrix = {
      Bitmatrix.init(rows, cols, initialValue)
    }

    def create(rows: Int, cols: Int, entries: Seq[(Int, Int, Boolean)], dense: Boolean): Bitmatrix = {
      val result = Bitmatrix.zeros(rows, cols)
      for ((row, col, value) <- entries) {
        result.update(row, col, value)
      }
      result
    }

    def eye(rows: Int, cols: Int, dense: Boolean): Bitmatrix = {
      val result = Bitmatrix.zeros(rows, cols)
      for (idx <- 0 until math.min(rows, cols)) {
        result.update(idx, idx, value = true)
      }
      result
    }

    def eye(rows: Int, cols: Int, startRow: Int, startCol: Int, dense: Boolean): Bitmatrix = {
      val result = Bitmatrix.zeros(rows, cols)

      for(idx <- 0 until math.min(rows - startRow, cols - startCol)){
        result.update(idx+startRow, idx+startCol, value = true)
      }

      result
    }
  }
}