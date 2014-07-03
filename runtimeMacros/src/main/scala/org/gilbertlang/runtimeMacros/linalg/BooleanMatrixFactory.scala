package org.gilbertlang.runtimeMacros.linalg

trait BooleanMatrixFactory extends Serializable {
  def create(rows: Int, cols: Int, dense: Boolean): BooleanMatrix
  def create(rows: Int, cols: Int, entries: Traversable[(Int, Int, Boolean)] , dense: Boolean): BooleanMatrix
  def init(rows: Int, cols: Int, intialValue: Boolean, dense: Boolean): BooleanMatrix
  def eye(row: Int, cols: Int, startRow: Int, startColumn: Int, dense: Boolean): BooleanMatrix

}
