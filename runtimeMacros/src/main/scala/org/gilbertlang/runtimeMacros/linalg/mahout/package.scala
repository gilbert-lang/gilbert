package org.gilbertlang.runtimeMacros.linalg

import org.apache.mahout.math._
import org.apache.mahout.math.function.{VectorFunction, DoubleDoubleFunction, DoubleFunction}


package object mahout {

  implicit def double2MahoutDouble(matrix: DoubleMatrix): MahoutDoubleMatrix = {
    require(matrix.isInstanceOf[MahoutDoubleMatrix])
    matrix.asInstanceOf[MahoutDoubleMatrix]
  }

  implicit def boolean2MahoutBoolean(matrix: BooleanMatrix): MahoutBooleanMatrix = {
    require(matrix.isInstanceOf[MahoutBooleanMatrix])
    matrix.asInstanceOf[MahoutBooleanMatrix]
  }

  implicit def double2MahoutVector(vector: DoubleVector): MahoutDoubleVector = {
    require(vector.isInstanceOf[MahoutDoubleVector])
    vector.asInstanceOf[MahoutDoubleVector]
  }

  implicit def func2DoubleFunc(func: Double => Double): DoubleFunction = {
    new DoubleFunction{
      override def apply(x: Double): Double = {
        func(x)
      }
    }
  }

  implicit def func2DoubleDoubleFunc(func: (Double, Double) => Double): DoubleDoubleFunction = {
    new DoubleDoubleFunction{
      override def apply(x: Double, y: Double): Double = {
        func(x,y)
      }
    }
  }

  implicit def func2VectorFunc(func: Vector => Double): VectorFunction = {
    new VectorFunction() {
      override def apply(v: Vector): Double = {
        func(v)
      }
    }
  }

   def b2D(v: Boolean): Double = {
     if (v) 1.0 else 0.0
   }

  def d2B(v: Double): Boolean = {
    if(v == 0) false else true
  }

  implicit def decorateMatrix(matrix: Matrix): MatrixDecorator = {
    new MatrixDecorator(matrix)
  }

  class MatrixDecorator(val matrix: Matrix){
    def copy(): Matrix = {
      matrix match {
        case m: DenseMatrix => m.clone()
        case m: SparseMatrix => m.clone()
        case _ => throw new IllegalArgumentException("Cannot copy matrix of type " + matrix.getClass)
      }
    }
  }

  implicit def decorateVector(vector: Vector): VectorDecorator = {
    new VectorDecorator(vector)
  }

  class VectorDecorator(val vector: Vector){
    def copy(): Vector = {
      vector match {
        case v: AbstractVector => v.clone()
        case _ => throw new IllegalArgumentException("Cannot copy vector of type " + vector.getClass)
      }
    }
  }
}
