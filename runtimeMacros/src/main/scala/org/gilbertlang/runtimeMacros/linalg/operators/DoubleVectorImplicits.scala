package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.linalg.support.{CanMapValues, CanZipMapValues}
import org.gilbertlang.runtimeMacros.linalg.DoubleVector
import breeze.linalg.{sum, min, max, norm, Vector}
import org.gilbertlang.runtimeMacros.linalg.breeze.BreezeDoubleVector
import org.gilbertlang.runtimeMacros.linalg.mahout.MahoutDoubleVector

trait DoubleVectorImplicits {
  implicit def handholdDV: CanMapValues.HandHold[DoubleVector, Double] = new CanMapValues.HandHold[DoubleVector, Double]

  implicit def canZipMapValuesDV: CanZipMapValues[DoubleVector, Double, Double, DoubleVector] = {
    new CanZipMapValues[DoubleVector, Double, Double, DoubleVector]{
      def map(from1: DoubleVector, from2: DoubleVector, fn: (Double, Double) => Double): DoubleVector = {
        (from1, from2) match {
          case (x: BreezeDoubleVector, y: BreezeDoubleVector) =>
            val zipper = implicitly[CanZipMapValues[Vector[Double], Double, Double, Vector[Double]]]
            zipper.map(x.vector, y.vector, fn):BreezeDoubleVector
          case (x: MahoutDoubleVector, y: MahoutDoubleVector) =>
            val result = x.vector.like()
            for(i <- 0 until x.vector.size){
              result.setQuick(i, fn(x.vector.getQuick(i), y.vector.getQuick(i)))
            }
            MahoutDoubleVector(result)
          case _ =>
            throw new IllegalArgumentException("Cannot zip map values for classes (" + from1.getClass + ", " +
              "" + from2.getClass + ")")
        }
      }
    }
  }

  implicit def canNormDV: norm.Impl2[DoubleVector, Double, Double] = {
    new norm.Impl2[DoubleVector, Double, Double] {
      def apply(v: DoubleVector, n: Double): Double = {
        v match {
          case v: BreezeDoubleVector =>
            norm(v.vector, n)
          case v: MahoutDoubleVector =>
            v.vector.norm(n)
          case _ =>
            throw new IllegalArgumentException("Does not support norm for " + v.getClass)
        }
      }
    }
  }

  implicit def canMaxDV: max.Impl[DoubleVector, Double] = {
    new max.Impl[DoubleVector, Double] {
      def apply(v: DoubleVector): Double = {
        v match {
          case v: BreezeDoubleVector =>
            max(v.vector)
          case v: MahoutDoubleVector =>
            v.vector.maxValue()
          case _ =>
            throw new IllegalArgumentException("Does not support max for " + v.getClass)
        }
      }
    }
  }

  implicit def canMinDV: min.Impl[DoubleVector, Double] = {
    new min.Impl[DoubleVector, Double] {
      def apply(v: DoubleVector): Double = {
        v match {
          case v: BreezeDoubleVector =>
            min(v.vector)
          case v: MahoutDoubleVector =>
            v.vector.minValue()
          case _ =>
            throw new IllegalArgumentException("Does not support min for " + v.getClass)
        }
      }
    }
  }

  implicit def canSumDV: sum.Impl[DoubleVector, Double] = {
    new sum.Impl[DoubleVector, Double] {
      def apply(v: DoubleVector): Double = {
        v match {
          case v: BreezeDoubleVector =>
            sum(v.vector)
          case v: MahoutDoubleVector =>
            v.vector.zSum()
          case _ =>
            throw new IllegalArgumentException("Does not support sum for " + v.getClass)
        }
      }
    }
  }
}
