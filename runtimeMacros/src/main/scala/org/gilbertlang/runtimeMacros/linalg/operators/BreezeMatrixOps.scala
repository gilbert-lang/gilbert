package org.gilbertlang.runtimeMacros.linalg.operators

import breeze.linalg._
import breeze.linalg.operators.OpLT
import breeze.linalg.operators.OpGT
import breeze.linalg.operators.OpEq
import breeze.linalg.operators.OpNe
import breeze.macros.expand
import breeze.linalg.operators.OpLTE
import breeze.linalg.operators.OpGTE
import org.gilbertlang.runtimeMacros.linalg.Bitmatrix

trait BreezeMatrixOps  {
   
  @expand
  @expand.valify
  implicit def comp_MM[@expand.args(Double) T, @expand.args(OpLT, OpLTE, OpGT, OpGTE, OpEq, OpNe) Op <: OpType](
      implicit @expand.sequence[Op]( {_ < _ }, {_ <= _}, {_ > _}, {_ >= _}, {_ == _}, {_ != _})
      comp: Op.Impl2[T, T, Boolean]): Op.Impl2[Matrix[T], Matrix[T], Bitmatrix] = 
      new Op.Impl2[Matrix[T], Matrix[T],Bitmatrix]{
    override def apply(a: Matrix[T], b: Matrix[T]): Bitmatrix = {
      val result = new Bitmatrix(a.rows, a.cols)
      
      for(col <- 0 until a.cols; row <- 0 until a.rows){
        result.update(row, col, comp(a(row,col), b(row, col)))
      }
      
      result
    }
  }

  @expand
  @expand.valify
  implicit def comp_MS[@expand.args(Double) T, @expand.args(OpLT, OpLTE, OpGT, OpGTE, OpEq, OpNe) Op <: OpType](
      implicit @expand.sequence[Op]( {_ < _ }, {_ <= _}, {_ > _}, {_ >= _}, {_ == _}, {_ != _})
      comp: Op.Impl2[T, T, Boolean]): Op.Impl2[Matrix[T], T, Bitmatrix] = 
      new Op.Impl2[Matrix[T], T,Bitmatrix]{
    override def apply(a: Matrix[T], b: T): Bitmatrix = {
      val result = new Bitmatrix(a.rows, a.cols)
      
      for(col <- 0 until a.cols; row <- 0 until a.rows){
        result.update(row, col, comp(a(row,col), b))
      }
      
      result
    }
  }
}