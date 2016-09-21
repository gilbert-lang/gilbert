package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.linalg.max
import breeze.linalg.support.{CanMapValues, CanZipMapValues}
import org.gilbertlang.runtimeMacros.linalg.{DoubleVector, Subvector}

trait SubvectorImplicits extends DoubleVectorImplicits {
  implicit def handholdSV: CanMapValues.HandHold[Subvector, Double] = new CanMapValues.HandHold[Subvector, Double]

  implicit def canZipMapValuesSV: CanZipMapValues[Subvector, Double, Double, Subvector] = {
    new CanZipMapValues[Subvector, Double, Double, Subvector]{
      def map(from1: Subvector, from2: Subvector, fn: (Double, Double) => Double): Subvector ={
        val zipper = implicitly[CanZipMapValues[DoubleVector, Double, Double, DoubleVector]]
        import from1._
        Subvector(zipper.map(from1.vector, from2.vector, fn), index, offset, totalEntries)
      }
    }
  }


  implicit def canMax: max.Impl[Subvector, Double] = {
    new max.Impl[Subvector, Double]{
      def apply(subvector: Subvector): Double = {
        max(subvector.vector)
      }
    }
  }
}
