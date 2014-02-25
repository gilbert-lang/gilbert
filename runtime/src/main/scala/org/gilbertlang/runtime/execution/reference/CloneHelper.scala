package org.gilbertlang.runtime.execution.reference

import org.apache.mahout.math.Matrix
import org.apache.mahout.math.DenseMatrix
import org.apache.mahout.math.SparseMatrix

object CloneHelper {
  def clone(matrix: Matrix): Matrix = {
    matrix match {
      case x: DenseMatrix => x.clone()
      case x: SparseMatrix => x.clone()
    }
  }
}