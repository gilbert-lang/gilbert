package org.gilbertlang.runtimeMacros.linalg


import _root_.breeze.linalg.support.{CanSlice2}

trait DoubleMatrix extends Serializable {
  def rows: Int

  def cols: Int

  def activeSize: Int

  def apply(x: Int, y: Int): Double

  def update(coord: (Int, Int), value: Double): Unit

  def activeIterator: Iterator[((Int, Int), Double)]

  def iterator: Iterator[((Int, Int), Double)]

  def mapActiveValues(func: Double => Double): DoubleMatrix


  def apply[Slice1, Slice2, Result](slice1: Slice1, slice2: Slice2)(implicit canSlice: CanSlice2[DoubleMatrix,
    Slice1, Slice2, Result]) = {
    canSlice(this, slice1, slice2)
  }

  def t: DoubleMatrix

  def +(op: DoubleMatrix): DoubleMatrix

  def -(op: DoubleMatrix): DoubleMatrix

  def /(op: DoubleMatrix): DoubleMatrix

  def :*(op: DoubleMatrix): DoubleMatrix

  def *(op: DoubleMatrix): DoubleMatrix

  def :^(op: DoubleMatrix): DoubleMatrix

  def +(sc: Double): DoubleMatrix

  def -(sc: Double): DoubleMatrix

  def /(sc: Double): DoubleMatrix

  def *(sc: Double): DoubleMatrix

  def :^(sc: Double): DoubleMatrix

  def :>(sc: Double): BooleanMatrix

  def :>=(sc: Double): BooleanMatrix

  def :<(sc: Double): BooleanMatrix

  def :<=(sc: Double): BooleanMatrix

  def :==(sc: Double): BooleanMatrix

  def :!=(sc: Double): BooleanMatrix

  def :>(op: DoubleMatrix): BooleanMatrix

  def :>=(op: DoubleMatrix): BooleanMatrix

  def :<(op: DoubleMatrix): BooleanMatrix

  def :<=(op: DoubleMatrix): BooleanMatrix

  def :==(op: DoubleMatrix): BooleanMatrix

  def :!=(op: DoubleMatrix): BooleanMatrix

  def copy: DoubleMatrix
}