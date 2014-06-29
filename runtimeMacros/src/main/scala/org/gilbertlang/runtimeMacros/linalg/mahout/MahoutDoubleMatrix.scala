package org.gilbertlang.runtimeMacros.linalg.mahout

import org.gilbertlang.runtimeMacros.linalg.DoubleMatrix
import org.apache.mahout.math.Matrix
import org.apache.mahout.math.function.Functions
import collection.JavaConversions._

class MahoutDoubleMatrix(var matrix: Matrix) extends DoubleMatrix {
  def this() = this(null)

  def rows: Int = matrix.numRows()
  def cols: Int = matrix.numCols()

  def apply(x: Int, y: Int): Double = matrix.get(x,y)

  def update(coord: (Int, Int), value: Double): Unit = matrix.set(coord._1, coord._2, value)

  def activeIterator: Iterator[((Int, Int), Double)] = {
    matrix.iterator().flatMap{ slice => slice.nonZeroes() map { element =>((slice.index(), element.index()),
      element.get())}}
  }

  def iterator: Iterator[((Int, Int), Double)] = {
    matrix.iterateAll().flatMap{ slice => slice.all() map { element => ((slice.index(), element.index()),
      element.get())}}
  }

  def mapActiveValues(func: Double => Double): MahoutDoubleMatrix = {
    MahoutDoubleMatrix(matrix.like().assign(func))
  }

  def copy = MahoutDoubleMatrix(matrix.like())

  def t = MahoutDoubleMatrix(matrix.transpose())

  def +(op: DoubleMatrix): MahoutDoubleMatrix = {
    MahoutDoubleMatrix(matrix.plus(op.matrix))
  }

  def -(op: DoubleMatrix): MahoutDoubleMatrix = {
    MahoutDoubleMatrix(matrix.minus(op.matrix))
  }

  def /(op: DoubleMatrix): MahoutDoubleMatrix = {
    MahoutDoubleMatrix(matrix.copy().assign(op.matrix, Functions.DIV))
  }

  def :*(op: DoubleMatrix): MahoutDoubleMatrix = {
    MahoutDoubleMatrix(matrix.copy().assign(op.matrix, Functions.MULT))
  }

  def *(op: DoubleMatrix): MahoutDoubleMatrix = {
    MahoutDoubleMatrix(matrix.times(op.matrix))
  }

  def :^(op: DoubleMatrix): MahoutDoubleMatrix = {
    MahoutDoubleMatrix(matrix.copy().assign(op.matrix, Functions.POW))
  }

  def +(op: Double): MahoutDoubleMatrix = {
    MahoutDoubleMatrix(matrix.plus(op))
  }

  def -(op:Double): MahoutDoubleMatrix = {
    MahoutDoubleMatrix(matrix.copy().assign{ x:Double => x - op})
  }

  def /(op: Double): MahoutDoubleMatrix = {
    MahoutDoubleMatrix(matrix.copy().assign{ x:Double => x/op})
  }

  def *(op: Double): MahoutDoubleMatrix = {
    MahoutDoubleMatrix(matrix.times(op))
  }

  def :^(op: Double): MahoutDoubleMatrix = {
    MahoutDoubleMatrix(matrix.copy().assign{x: Double => math.pow(x, op)})
  }

  def :>(sc: Double): MahoutBooleanMatrix = {
    MahoutBooleanMatrix(matrix.copy().assign{x:Double => b2D(x>sc)})
  }

  def :>=(sc: Double): MahoutBooleanMatrix = {
    MahoutBooleanMatrix(matrix.copy().assign{x:Double => b2D(x>= sc)})
  }

  def :<(sc: Double): MahoutBooleanMatrix = {
    MahoutBooleanMatrix(matrix.copy().assign{x: Double => b2D(x < sc)})
  }

  def :<=(sc: Double): MahoutBooleanMatrix = {
    MahoutBooleanMatrix(matrix.copy().assign{x:Double => b2D(x <= sc)})
  }

  def :==(sc: Double): MahoutBooleanMatrix = {
    MahoutBooleanMatrix(matrix.copy().assign{x: Double => b2D(x == sc)})
  }

  def :!=(sc: Double): MahoutBooleanMatrix = {
    MahoutBooleanMatrix(matrix.copy().assign{x: Double => b2D(x!= sc)})
  }

  def :>(op: DoubleMatrix): MahoutBooleanMatrix = {
    MahoutBooleanMatrix(matrix.copy().assign(op.matrix, Functions.GREATER))
  }

  def :>=(op: DoubleMatrix): MahoutBooleanMatrix = {
    MahoutBooleanMatrix(matrix.copy().assign(op.matrix, (x: Double, y: Double) => b2D(x >= y)))
  }

  def :<(op: DoubleMatrix): MahoutBooleanMatrix = {
    MahoutBooleanMatrix(matrix.copy().assign(op.matrix, Functions.LESS))
  }

  def :<=(op: DoubleMatrix): MahoutBooleanMatrix = {
    MahoutBooleanMatrix(matrix.copy().assign(op.matrix, (x: Double, y: Double) => b2D(x <= y)))
  }

  def :==(op: DoubleMatrix): MahoutBooleanMatrix = {
    MahoutBooleanMatrix(matrix.copy().assign(op.matrix, Functions.EQUALS))
  }

  def :!=(op: DoubleMatrix): MahoutBooleanMatrix = {
    MahoutBooleanMatrix(matrix.copy().assign(op.matrix, (x: Double, y: Double) => b2D(x != y)))
  }
}

object MahoutDoubleMatrix{
  def apply(matrix: Matrix): MahoutDoubleMatrix = {
    new MahoutDoubleMatrix(matrix)
  }
}
