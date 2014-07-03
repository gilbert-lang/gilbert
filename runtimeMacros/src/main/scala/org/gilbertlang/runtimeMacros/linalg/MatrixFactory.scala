package org.gilbertlang.runtimeMacros.linalg

object MatrixFactory {
  var doubleMatrixFactory: DoubleMatrixFactory = null
  var booleanMatrixFactory: BooleanMatrixFactory = null

  def getDouble: DoubleMatrixFactory = {
    if(doubleMatrixFactory == null){
      throw new IllegalArgumentException("The double matrix factory has not been set.")
    }else{
      doubleMatrixFactory
    }
  }

  def getBoolean: BooleanMatrixFactory = {
    if(booleanMatrixFactory == null){
      throw new IllegalArgumentException("The boolean matrix factory has not been set.")
    }else{
      booleanMatrixFactory
    }
  }


}
