package org.gilbertlang.runtimeMacros.linalg.mahout.operators

import breeze.linalg.Axis
import breeze.linalg.support.{CanCollapseAxis, CanTraverseValues}
import breeze.linalg.support.CanTraverseValues.ValuesVisitor
import org.apache.mahout.math.{Matrix, Vector}
import org.gilbertlang.runtimeMacros.linalg.mahout._
import collection.JavaConversions._

trait MahoutMatrixImplicits {
  implicit def canTraverseMM: CanTraverseValues[Matrix, Double] = {
    new CanTraverseValues[Matrix, Double] {
      override def isTraversableAgain(from: Matrix):Boolean = true

      override def traverse(from: Matrix, valuesVisitor: ValuesVisitor[Double]): Unit ={
        for(slice <- from.iterateAll()){
          for(element <- slice.vector().all()){
            valuesVisitor.visit(element.get())
          }
        }
      }
    }
  }

  implicit def canCollapseRowsMM: CanCollapseAxis[Matrix, Axis._0.type, Vector, Double, MahoutDoubleMatrix] = {
    new CanCollapseAxis[Matrix, Axis._0.type, Vector, Double, MahoutDoubleMatrix] {
      override def apply(from: Matrix, axis: Axis._0.type)(fn: Vector => Double) = {
        MahoutDoubleVector(from.aggregateColumns(fn)).asRowMatrix
      }
    }
  }

  implicit def canCollapseColsMM: CanCollapseAxis[Matrix, Axis._1.type, Vector, Double, MahoutDoubleVector] = {
    new CanCollapseAxis[Matrix, Axis._1.type, Vector, Double, MahoutDoubleVector] {
      override def apply(from: Matrix, axis: Axis._1.type)(fn: Vector => Double) = {
        MahoutDoubleVector(from.aggregateRows(fn))
      }
    }
  }
}
