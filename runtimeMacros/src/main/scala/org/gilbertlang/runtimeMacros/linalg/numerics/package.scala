package org.gilbertlang.runtimeMacros.linalg

import _root_.breeze.generic.UFunc
import _root_.breeze.generic.MappingUFunc
import _root_.breeze.linalg.Tensor

package object numerics {
  object max extends UFunc {
    implicit object maxDoubleDoubleImpl extends Impl2[Double, Double, Double] {
      override def apply(a: Double, b: Double): Double = {
        scala.math.max(a, b)
      }
    }

    implicit def maxTensor[T <: Tensor[_, Double]]: Impl[T, Double] = new Impl[T, Double] {
      override def apply(tensor: T): Double = {
        max(tensor)
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
        min(tensor)
      }
    }

  }
}

