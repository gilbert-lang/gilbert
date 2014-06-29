package org.gilbertlang.runtimeMacros.linalg.mahout

import org.apache.mahout.math.Matrix
import org.apache.mahout.math.function.Functions
import org.gilbertlang.runtimeMacros.linalg.BooleanMatrix
import collection.JavaConversions._

class MahoutBooleanMatrix(var matrix: Matrix) extends BooleanMatrix {
  def rows = matrix.numRows()
  def cols = matrix.numCols()

  def apply(row: Int, col:Int): Boolean = d2B(matrix.get(row, col))

  def update(coord: (Int, Int), value: Boolean): Unit = {
    matrix.set(coord._1, coord._2, b2D(value))
  }

  def activeIterator:Iterator[((Int, Int), Boolean)] = {
    matrix.iterator() flatMap { slice => slice.vector().nonZeroes().map { element => ((slice.index(),
      element.index()), d2B(element.get()))}}
  }

  def copy = MahoutBooleanMatrix(matrix.like())

  def &(op: BooleanMatrix): BooleanMatrix = {
    MahoutBooleanMatrix(matrix.copy().assign(op.matrix, Functions.MULT))
  }

  def &(op: Boolean): BooleanMatrix = {
    MahoutBooleanMatrix(matrix.copy().assign{x:Double => x * b2D(op)})
  }

  def t: MahoutBooleanMatrix = {
    MahoutBooleanMatrix(matrix.transpose())
  }
}

object MahoutBooleanMatrix{
  def apply(matrix: Matrix): MahoutBooleanMatrix = new MahoutBooleanMatrix(matrix)
}
