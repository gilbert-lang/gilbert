package org.gilbertlang.runtime.execution.stratosphere

import org.apache.mahout.math.Matrix
import org.apache.mahout.math.Vector
import org.apache.mahout.math.AbstractMatrix
import org.apache.mahout.math.AbstractVector

object Helper {
  def clone(matrix: Matrix): Matrix = {
    if( matrix.isInstanceOf[AbstractMatrix]){
      matrix.asInstanceOf[AbstractMatrix].clone()
    }else{
      throw new IllegalArgumentError("Cannot clone matrix of type " + matrix.getClass().getName() + ".")
    }
  }
  
  def clone(vector: Vector): Vector = {
    if(vector.isInstanceOf[AbstractVector]){
      vector.asInstanceOf[AbstractVector].clone()
    }else{
      throw new IllegalArgumentError("Cannot clone vector of type " + vector.getClass.getName + ".")
    }
  }
}