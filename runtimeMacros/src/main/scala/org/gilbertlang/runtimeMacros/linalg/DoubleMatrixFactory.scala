package org.gilbertlang.runtimeMacros.linalg

import _root_.breeze.stats.distributions.Rand


trait DoubleMatrixFactory {
  def create(rows: Int, cols: Int, dense: Boolean): DoubleMatrix
  def create(rows: Int, cols: Int, entries: Traversable[(Int, Int, Double)], dense: Boolean): DoubleMatrix
  def init(rows: Int, cols: Int, initialValue: Double, dense: Boolean): DoubleMatrix
  def eye(rows: Int, cols: Int, dense: Boolean): DoubleMatrix
  def eye(rows: Int, cols: Int, startRow: Int, startCol: Int, dense: Boolean): DoubleMatrix
  def rand(rows: Int, cols: Int, rand: Rand[Double]): DoubleMatrix
}
