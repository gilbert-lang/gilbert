package org.gilbertlang.runtimeMacros.linalg

import breeze.generic.UFunc
import breeze.generic.MappingUFunc
import breeze.linalg.Tensor

package object numerics {
  object max extends UFunc {
    implicit object maxDoubleDoubleImpl extends Impl2[Double, Double, Double] {
      override def apply(a: Double, b: Double): Double = {
        scala.math.max(a, b)
      }
    }

    implicit def maxTensor[T <: Tensor[_, Double]]: Impl[T, Double] = new Impl[T, Double] {
      override def apply(tensor: T): Double = {
        tensor.max
      }
    }

  }

  object min extends UFunc {
    implicit object minDoubleDoubleImpl extends Impl2[Double, Double, Double] {
      override def apply(a: Double, b: Double): Double = {
        scala.math.min(a, b)
      }
    }

    implicit def minTensor[T <: Tensor[_, Double]]: Impl[T, Double] = new Impl[T, Double] {
      override def apply(tensor: T): Double = {
        tensor.min
      }
    }

  }
}

