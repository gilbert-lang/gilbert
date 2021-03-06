package org.gilbertlang.runtimeMacros.linalg.mahout

import breeze.stats.distributions.Rand
import org.apache.mahout.math.{SparseMatrix, DenseMatrix}
import org.gilbertlang.runtimeMacros.linalg.DoubleMatrixFactory

object MahoutDoubleMatrixFactory extends DoubleMatrixFactory {
  def create(rows: Int, cols: Int, entries: Traversable[(Int, Int, Double)], dense: Boolean): MahoutDoubleMatrix = {
    if(dense){
      val data = Array.ofDim[Double](rows, cols)

      entries foreach {
        case (row, col, value) =>
          data(row)(col) = value
      }

      MahoutDoubleMatrix(new DenseMatrix(data, true))
    }else{
      val result = new SparseMatrix(rows, cols)

      entries foreach{
        case (row, col, value) =>
          result.setQuick(row, col, value)
      }

      MahoutDoubleMatrix(result)
    }
  }

  def create(rows: Int, cols: Int, dense: Boolean): MahoutDoubleMatrix = {
    MahoutDoubleMatrix(if(dense){
      new DenseMatrix(rows, cols)
    }else{
      new SparseMatrix(rows, cols)
    })
  }

  def eye(rows: Int, cols: Int, startRow: Int, startColumn: Int, dense:Boolean): MahoutDoubleMatrix = {
    val result = if(dense){
      new DenseMatrix(rows, cols)
    }else{
      new SparseMatrix(rows, cols)
    }

    for(i <- 0 until math.min(rows-startRow, cols-startColumn)){
      result.setQuick(i+startRow, i+ startColumn, 1)
    }

    MahoutDoubleMatrix(result)
  }

  def eye(rows: Int, cols: Int, dense: Boolean): MahoutDoubleMatrix = {
    eye(rows, cols, 0, 0, dense)
  }

  def init(rows: Int, cols: Int, initialValue: Double, dense: Boolean): MahoutDoubleMatrix = {
    if(dense){
      val data = Array.fill(rows, cols)(initialValue)

      MahoutDoubleMatrix(new DenseMatrix(data, true))
    }else{
      val result = new SparseMatrix(rows, cols)

      for(row <- 0 until rows; col <- 0 until cols){
        result.setQuick(row, col, initialValue)
      }

      MahoutDoubleMatrix(result)
    }
  }

  def rand(rows: Int, cols: Int, rand: Rand[Double]): MahoutDoubleMatrix = {
    val result = new DenseMatrix(rows, cols)

    for(row <- 0 until rows; col <- 0 until cols){
      result.setQuick(row, col, rand.sample())
    }

    MahoutDoubleMatrix(result)
  }

  def sprand(rows: Int, cols: Int, rand: Rand[Double], level: Double): MahoutDoubleMatrix = {
    val result = new SparseMatrix(rows, cols)
    val uniform = Rand.uniform

    for(r <- 0 until rows; c <- 0 until cols){
      if(uniform.draw() < level) {
        result.setQuick(r, c, rand.draw())
      }
    }

    MahoutDoubleMatrix(result)
  }

  def adaptiveRand(rows: Int, cols: Int, rand: Rand[Double], level: Double,
                   denseThreshold: Double): MahoutDoubleMatrix = {
    val uniform = Rand.uniform

    val coordinates = for(r <- 0 until rows; c <- 0 until cols) yield {
      (r, c)
    }

    val entries = coordinates zip (uniform.sample(rows*cols)) filter { t => t._2 < level } map { case ((row, col),
    _) => (row, col,rand.draw())}

    create(rows, cols, entries, (entries.size.toDouble/(rows*cols))> denseThreshold)
  }
}
