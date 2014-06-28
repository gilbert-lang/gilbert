package org.gilbertlang.runtimeMacros.linalg.breeze

import breeze.stats.distributions.Rand
import org.gilbertlang.runtimeMacros.linalg.{BooleanMatrix, BooleanMatrixFactory}

object BreezeBooleanMatrixFactory extends BooleanMatrixFactory{
  def create(rows: Int, cols: Int, dense: Boolean): BooleanMatrix = {
    Bitmatrix.zeros(rows, cols)
  }

  def init(rows: Int, cols: Int, initialValue: Boolean, dense: Boolean): BooleanMatrix = {
    Bitmatrix.init(rows, cols, initialValue)
  }

  def create(rows: Int, cols: Int, entries: Traversable[(Int, Int, Boolean)], dense: Boolean): BooleanMatrix = {
    val result = Bitmatrix.zeros(rows, cols)
    for ((row, col, value) <- entries) {
      result.update(row, col, value)
    }
    result
  }

  def eye(rows: Int, cols: Int, dense: Boolean): BooleanMatrix = {
    val result = Bitmatrix.zeros(rows, cols)
    for (idx <- 0 until math.min(rows, cols)) {
      result.update(idx, idx, value = true)
    }
    result
  }

  def eye(rows: Int, cols: Int, startRow: Int, startCol: Int, dense: Boolean): BooleanMatrix = {
    val result = Bitmatrix.zeros(rows, cols)

    for(idx <- 0 until math.min(rows - startRow, cols - startCol)){
      result.update(idx+startRow, idx+startCol, value = true)
    }

    result
  }

  def rand(rows: Int, cols: Int) = {
    Bitmatrix.rand(rows, cols, Rand.uniform)
  }
}
