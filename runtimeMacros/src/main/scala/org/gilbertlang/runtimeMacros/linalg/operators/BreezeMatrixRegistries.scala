package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.macros.expand
import breeze.linalg.operators.BinaryUpdateRegistry
import breeze.linalg.Matrix
import breeze.linalg.operators.BinaryRegistry
import breeze.linalg.operators.OpAnd
import breeze.linalg.operators.OpOr
import breeze.linalg.operators.OpType

trait BreezeMatrixRegistries {
  @expand
  @expand.valify
  implicit def logicalOp_MMUpdate[@expand.args(OpOr, OpAnd) Op <: OpType](implicit 
      @expand.sequence[Op]({_ || _},{_ && _}) op: Op.Impl2[Boolean, Boolean, Boolean]):
      BinaryUpdateRegistry[Matrix[Boolean], Matrix[Boolean], Op.type] =
      new BinaryUpdateRegistry[Matrix[Boolean], Matrix[Boolean], Op.type]{
    override def bindingMissing(a: Matrix[Boolean], b: Matrix[Boolean]){
      require(a.rows == b.rows, "Matrixs must have the same number of rows")
      require(a.cols == b.cols, "Matrixs must have the same number of cols")
      
      for(col <- 0 until a.cols; row <- 0 until a.rows){
        a.update(row, col, op(a(row,col), b(row, col)))
      }
    }
  }

  @expand
  @expand.valify
  implicit def logicalOp_MSUpdate[@expand.args(OpOr, OpAnd) Op <: OpType](implicit 
      @expand.sequence[Op]({_ || _},{_ && _}) op: Op.Impl2[Boolean, Boolean, Boolean]):
      BinaryUpdateRegistry[Matrix[Boolean], Boolean, Op.type] =
      new BinaryUpdateRegistry[Matrix[Boolean], Boolean, Op.type]{
    override def bindingMissing(a: Matrix[Boolean], b: Boolean){
      
      for(col <- 0 until a.cols; row <- 0 until a.rows){
        a.update(row, col, op(a(row,col), b))
      }
    }
  }
  
  @expand
  @expand.valify
  implicit def logicalOp_MM[@expand.args(OpOr, OpAnd) Op <: OpType]: 
    BinaryRegistry[Matrix[Boolean], Matrix[Boolean], Op.type, Matrix[Boolean]] = {
    val uop = implicitly[Op.InPlaceImpl2[Matrix[Boolean], Matrix[Boolean]]]
    new BinaryRegistry[Matrix[Boolean], Matrix[Boolean], Op.type, Matrix[Boolean]]{
      override def bindingMissing(a: Matrix[Boolean], b: Matrix[Boolean]): Matrix[Boolean] = {
        val c = breeze.linalg.copy(a)
        uop(c,b)
        c
      }
    }
  }

  @expand
  @expand.valify
  implicit def logicalOp_MS[@expand.args(OpOr, OpAnd) Op <: OpType]: 
    BinaryRegistry[Matrix[Boolean], Boolean, Op.type, Matrix[Boolean]] = {
    val uop = implicitly[Op.InPlaceImpl2[Matrix[Boolean], Boolean]]
    new BinaryRegistry[Matrix[Boolean], Boolean, Op.type, Matrix[Boolean]]{
      override def bindingMissing(a: Matrix[Boolean], b: Boolean): Matrix[Boolean] = {
        val c = breeze.linalg.copy(a)
        uop(c,b)
        c
      }
    }
  }
}