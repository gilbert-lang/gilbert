package org.gilbertlang.runtimeMacros.linalg.breeze

import breeze.linalg.{DenseVector, SparseVector, Vector}
import org.gilbertlang.runtimeMacros.linalg.DoubleVector
import org.gilbertlang.runtimeMacros.linalg.breeze.operators.BreezeVectorImplicits

class BreezeDoubleVector(val vector: Vector[Double]) extends DoubleVector with BreezeVectorImplicits {

  def copy = new BreezeDoubleVector(vector.copy)

  def map(fn: Double => Double): BreezeDoubleVector = {
    vector map fn
  }

  def iterator: Iterator[(Int, Double)] = vector.iterator

  def +(op: DoubleVector): BreezeDoubleVector = {
    vector + op.vector
  }

  def :/=(value: Double): BreezeDoubleVector = {
    vector :/= value
  }

  def :/=(v: DoubleVector): BreezeDoubleVector = {
    vector :/= v.vector
  }

  def asMatrix: BreezeDoubleMatrix = {
    vector match {
      case v: DenseVector[Double] =>
        v.asDenseMatrix.t
      case v: SparseVector[Double] =>
        v.asSparseMatrix
      case _ =>
        throw new IllegalArgumentException("Cannot construct a matrix out of " + vector.getClass)
    }
  }
}
