package org.gilbertlang.runtimeMacros.linalg

trait DoubleVector extends Serializable{
  def copy: DoubleVector

  def iterator: Iterator[(Int, Double)]

  def map(fn: Double => Double): DoubleVector

  def :/=(value: Double): DoubleVector
  def :/=(v : DoubleVector): DoubleVector

  def +(vector: DoubleVector): DoubleVector

  def asMatrix: DoubleMatrix
}
