package org.gilbertlang.runtimeMacros.linalg.breeze

import breeze.linalg._
import org.gilbertlang.runtimeMacros.linalg.breeze.operators.{BreezeMatrixImplicits, BreezeMatrixOps}
import org.gilbertlang.runtimeMacros.linalg.{BooleanMatrix, DoubleMatrix}


class BreezeDoubleMatrix(var matrix: Matrix[Double]) extends DoubleMatrix{
  import BreezeDoubleMatrix._
  def this() = { this(null) }

  def rows: Int = matrix.rows
  def cols: Int = matrix.cols

  def apply(x: Int, y: Int): Double = matrix(x, y)
  def update(coord:(Int,Int), value: Double) {
    matrix.update(coord, value)
  }

  def activeIterator:Iterator[((Int, Int), Double)] = matrix.activeIterator
  def iterator:Iterator[((Int, Int), Double)] = matrix.iterator

  def mapActiveValues(func: Double => Double) = matrix.mapActiveValues(func)

  def t = BreezeDoubleMatrix(matrix.t)

  def +(op: DoubleMatrix): DoubleMatrix = {
    matrix + op.matrix
  }

  def -(op: DoubleMatrix): DoubleMatrix = {
    matrix - op.matrix
  }
  def /(op: DoubleMatrix): DoubleMatrix = {
    matrix / op.matrix
  }
  def *(op: DoubleMatrix): DoubleMatrix = {
    matrix * op.matrix
  }

  def :*(op: DoubleMatrix): DoubleMatrix = {
    matrix :* op.matrix
  }

  def :^(op: DoubleMatrix): DoubleMatrix = {
    matrix :^ op.matrix
  }

  def +(sc: Double): DoubleMatrix = { matrix + sc }
  def -(sc: Double): DoubleMatrix = { matrix - sc }
  def /(sc: Double): DoubleMatrix = { matrix / sc }
  def *(sc: Double): DoubleMatrix = { matrix * sc }
  def :^(sc: Double): DoubleMatrix = { matrix :^ sc }

  def :>(sc: Double): BooleanMatrix = { matrix :> sc }
  def :>=(sc: Double): BooleanMatrix = { matrix :>= sc }
  def :<(sc: Double): BooleanMatrix = { matrix :< sc }
  def :<=(sc: Double): BooleanMatrix = { matrix :<= sc }
  def :==(sc: Double): BooleanMatrix = { matrix :== sc }
  def :!=(sc:Double): BooleanMatrix = { matrix :!= sc }

  def :>(op: DoubleMatrix): BooleanMatrix = { matrix :> op.matrix }
  def :>=(op: DoubleMatrix): BooleanMatrix = { matrix :>= op.matrix }
  def :<(op: DoubleMatrix): BooleanMatrix = { matrix :< op.matrix }
  def :<=(op: DoubleMatrix): BooleanMatrix = { matrix :<= op.matrix}
  def :==(op: DoubleMatrix): BooleanMatrix = { matrix :== op.matrix }
  def :!=(op: DoubleMatrix): BooleanMatrix = { matrix :!= op.matrix }

  def copy:DoubleMatrix = { new BreezeDoubleMatrix(matrix.copy) }

  override def toString = {
    matrix.toString()
  }

}

object BreezeDoubleMatrix extends BreezeMatrixOps with BreezeMatrixImplicits{
  def apply(matrix: Matrix[Double]): BreezeDoubleMatrix = { new BreezeDoubleMatrix(matrix)}
}
