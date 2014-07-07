package org.gilbertlang.runtimeMacros.linalg.breeze

import org.gilbertlang.runtimeMacros.linalg.{BooleanMatrix}

class BreezeBooleanMatrix(var matrix: Bitmatrix) extends BooleanMatrix {
  def rows: Int = matrix.rows
  def cols: Int = matrix.cols

  def activeSize: Int = matrix.activeSize

  def apply(x: Int, y: Int): Boolean = matrix(x, y)
  def update(coord:(Int,Int), value: Boolean) {
    matrix.update(coord, value)
  }

  def &(op: BooleanMatrix): BooleanMatrix = {
    matrix & op.matrix
  }

  def &(sc: Boolean): BooleanMatrix = {
    matrix & sc
  }

  def :|(op: BooleanMatrix): BooleanMatrix = {
    matrix :| op.matrix
  }

  def :|(sc: Boolean): BooleanMatrix = {
    matrix :| sc
  }

  def t: BooleanMatrix = {
    matrix.t
  }

  def activeIterator:Iterator[((Int, Int), Boolean)] = matrix.activeIterator

  def copy:BooleanMatrix = { new BreezeBooleanMatrix(matrix.copy) }

  override def toString = matrix.toString
}

object BreezeBooleanMatrix{
  def apply(m: Bitmatrix) = new BreezeBooleanMatrix(m)
}


