package org.gilbertlang.runtimeMacros.linalg.mahout

import org.apache.mahout.math.{SparseMatrix, DenseMatrix}
import org.gilbertlang.runtimeMacros.linalg.{BooleanMatrix, BooleanMatrixFactory}

object MahoutBooleanMatrixFactory extends BooleanMatrixFactory{
  def create(rows: Int, cols: Int, entries: Traversable[(Int, Int, Boolean)], dense: Boolean): BooleanMatrix = {
    if(dense){
      val data = Array.ofDim[Double](rows, cols)

      entries foreach {
        case (row, col, value) =>
          data(row)(col) = b2D(value)
      }
      MahoutBooleanMatrix(new DenseMatrix(data, true))
    }else{
      val result = new SparseMatrix(rows, cols)

      entries foreach {
        case (row, col, value) =>
          result.setQuick(row, col, b2D(value))
      }
      MahoutBooleanMatrix(result)
    }
  }

  def create(rows: Int, cols: Int, dense:Boolean): MahoutBooleanMatrix = {
    if(dense){
      MahoutBooleanMatrix(new DenseMatrix(rows, cols))
    }else{
      MahoutBooleanMatrix(new SparseMatrix(rows, cols))
    }
  }

  def eye(rows: Int, cols: Int, startRow: Int, startColumn: Int, dense: Boolean): MahoutBooleanMatrix = {
    val result = if(dense){
      new DenseMatrix(rows, cols)
    }else{
      new SparseMatrix(rows, cols)
    }

    for(i <- 0 until math.min(rows-startRow, cols-startColumn)){
      result.setQuick(startRow+i, startColumn+i, 1)
    }

    MahoutBooleanMatrix(result)
  }

  def init(rows: Int, cols: Int, initialValue: Boolean, dense: Boolean): MahoutBooleanMatrix = {
    if(dense){
      val data = Array.fill(rows, cols)(b2D(initialValue))

      MahoutBooleanMatrix(new DenseMatrix(data, true))
    }else{
      val result = new SparseMatrix(rows, cols)

      for(row <- 0 until rows; col <- 0 until cols){
        result.setQuick(row, col, b2D(initialValue))
      }

      MahoutBooleanMatrix(result)
    }
  }
}
