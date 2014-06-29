package org.gilbertlang.runtimeMacros.linalg.mahout

import org.apache.mahout.math._
import org.gilbertlang.runtimeMacros.linalg.DoubleVector
import collection.JavaConversions._

class MahoutDoubleVector(var vector: Vector) extends DoubleVector {
  def this() = this(null)

  def copy = MahoutDoubleVector(vector.copy())

  def iterator: Iterator[(Int, Double)] = {
    val t = vector.all() map { element => (element.index(), element.get())}
    t.toIterator
  }

  def map(fn: Double => Double): MahoutDoubleVector = {
    MahoutDoubleVector(vector.copy().assign(fn))
  }

  def :/=(value: Double): MahoutDoubleVector = {
    vector.assign{x:Double => x/value}
    this
  }

  def :/=(v: DoubleVector): DoubleVector = {
    vector.assign(v.vector, {(x: Double, y: Double) => x/y})
    this
  }

  def +(v: DoubleVector): DoubleVector = {
   MahoutDoubleVector(vector.plus(v.vector))
  }

  def asMatrix: MahoutDoubleMatrix = {
    val matrix = vector match {
      case v:DenseVector =>
        val data = Array.ofDim[Double](v.size(), 1)

        for(element <- v.all()){
          data(element.index())(0) = element.get()
        }

        new DenseMatrix(data)
      case v:RandomAccessSparseVector =>
        val m = new SparseMatrix(v.size(), 1)
        for(element <- v.iterateNonZero()){
          m.setQuick(element.index(), 0, element.get())
        }
        m
      case _ =>
        val temp = new DenseVector(1)
        temp.setQuick(0, 1)
        vector.cross(temp)
    }

    MahoutDoubleMatrix(matrix)
  }

  def asRowMatrix: MahoutDoubleMatrix = {
    val matrix = vector match {
      case v: DenseVector =>
        val data = Array.ofDim[Double](1, v.size())

        for(element <- v.all()){
          data(0)(element.index()) = element.get()
        }

        new DenseMatrix(data)
      case v: RandomAccessSparseVector =>
        val map = scala.collection.mutable.Map[Integer, RandomAccessSparseVector]()
        map.add((0, v))
        new SparseMatrix(1, v.size(), map)
      case _ =>
        val temp = new DenseMatrix(1, vector.size())
        for(i <- 0 until vector.size()){
          temp.setQuick(0, i, vector.getQuick(i))
        }
        temp
    }

    MahoutDoubleMatrix(matrix)
  }
}

object MahoutDoubleVector{
  def apply(vector: Vector) = new MahoutDoubleVector(vector)
}
