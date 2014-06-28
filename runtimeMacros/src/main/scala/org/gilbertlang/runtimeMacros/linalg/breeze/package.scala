package org.gilbertlang.runtimeMacros.linalg

import _root_.breeze.linalg.Matrix
import _root_.breeze.linalg.Vector
import scala.language.implicitConversions

package object breeze {
  implicit def breeze2GilbertMatrix(matrix: Matrix[Double]): BreezeDoubleMatrix = {
    new BreezeDoubleMatrix(matrix)
  }

  implicit def gilbert2BreezeMatrix(matrix: DoubleMatrix): BreezeDoubleMatrix = {
    require(matrix.isInstanceOf[BreezeDoubleMatrix])
    matrix.asInstanceOf[BreezeDoubleMatrix]
  }

  implicit def breezeBoolean2GilbertMatrix(matrix: Bitmatrix): BooleanMatrix = {
    new BreezeBooleanMatrix(matrix)
  }

  implicit def gilbertBoolean2BreezeMatrix(matrix: BooleanMatrix): BreezeBooleanMatrix = {
    require(matrix.isInstanceOf[BreezeBooleanMatrix])
    matrix.asInstanceOf[BreezeBooleanMatrix]
  }

  implicit def breeze2GilbertVector(vector: Vector[Double]): BreezeDoubleVector = {
    new BreezeDoubleVector(vector)
  }

  implicit def gilbert2BreezeVector(vector: DoubleVector): BreezeDoubleVector = {
    require(vector.isInstanceOf[BreezeDoubleVector])
    vector.asInstanceOf[BreezeDoubleVector]
  }
}
