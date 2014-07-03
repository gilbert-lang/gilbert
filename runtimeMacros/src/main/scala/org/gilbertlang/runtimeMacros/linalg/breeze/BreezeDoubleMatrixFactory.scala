package org.gilbertlang.runtimeMacros.linalg.breeze

import breeze.stats.distributions.Rand
import org.gilbertlang.runtimeMacros.linalg.{DoubleMatrix, DoubleMatrixFactory}
import breeze.linalg.{DenseMatrix => BreezeDenseMatrix, CSCMatrix => BreezeSparseMatrix}

@SerialVersionUID(1)
object BreezeDoubleMatrixFactory extends DoubleMatrixFactory {
  def create(rows: Int, cols: Int, dense: Boolean): DoubleMatrix = {
    if(dense){
      BreezeDenseMatrix.zeros[Double](rows, cols)
    }else{
      BreezeSparseMatrix.zeros[Double](rows, cols)
    }
  }

  def init(rows: Int, cols: Int, initialValue: Double, dense: Boolean): DoubleMatrix = {
    val data = Array.fill[Double](rows*cols)(initialValue)
    if(dense){
      BreezeDenseMatrix.create[Double](rows, cols, data)
    }else{
      BreezeSparseMatrix.create[Double](rows, cols, data)
    }
  }

  def create(rows: Int, cols: Int, entries: Traversable[(Int,Int,Double)], dense: Boolean): DoubleMatrix = {
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

  def eye(rows: Int, cols: Int, dense: Boolean): DoubleMatrix = {
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

  def eye(rows: Int, cols: Int, startRow: Int, startCol: Int, dense: Boolean): DoubleMatrix = {
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

  def rand(rows: Int, cols: Int, rand: Rand[Double]) = {
    BreezeDenseMatrix.rand[Double](rows, cols, rand)
  }

  def sprand(rows: Int, cols: Int, rand: Rand[Double], level: Double) = {
    val builder = new BreezeSparseMatrix.Builder[Double](rows, cols, (level*rows*cols).toInt)
    val uniform = Rand.uniform
    for(r <- 0 until rows; c <- 0 until cols){
      if(uniform.draw() < level){
        builder.add(r, c, rand.draw())
      }
    }

    builder.result
  }

  def adaptiveRand(rows: Int, cols: Int, rand: Rand[Double], level: Double,
                   denseThreshold: Double): BreezeDoubleMatrix = {
    val uniform = Rand.uniform

    uniform.sample(rows*cols).filter( d => d < level)

    val coordinates = for(r <- 0 until rows; c <- 0 until cols) yield {
        (r, c)
    }

    val entries = coordinates zip (uniform.sample(rows*cols)) filter { t => t._2 < level } map { case ((row, col),
    _) => (row, col,rand.draw())}

    create(rows, cols, entries, (entries.size.toDouble/(rows*cols)) >= denseThreshold)
  }

}
