package org.gilbertlang.runtimeMacros.linalg.breeze.operators

import breeze.linalg._
import breeze.linalg.operators.{OpEq, OpGT, OpGTE, OpLT, OpLTE, OpNe}
import breeze.linalg.support.{CanMapActiveValues, CanMapValues}
import breeze.macros.expand
import org.gilbertlang.runtimeMacros.linalg.breeze.Bitmatrix

trait BreezeMatrixOps extends BreezeSparseMatrixImplicits  {
   
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

  implicit def canMapValues: CanMapValues[Matrix[Double], Double, Double, Matrix[Double]] = {
    new CanMapValues[Matrix[Double], Double, Double, Matrix[Double]]{
      def apply(from : Matrix[Double], func: Double => Double): Matrix[Double] = {
        from match {
          case m: DenseMatrix[Double] => m.map(func)
          case m: CSCMatrix[Double] => m.map(func)
          case _ => throw new IllegalArgumentException("Type " + from.getClass + " is not supported.")
        }
      }
    }
  }

  implicit def canMapActiveValues: CanMapActiveValues[Matrix[Double], Double, Double, Matrix[Double]] = {
    new CanMapActiveValues[Matrix[Double], Double, Double, Matrix[Double]] {
      def apply(from: Matrix[Double], func: Double => Double): Matrix[Double] = {
        from match {
          case m: DenseMatrix[Double] => m.mapActiveValues(func)
          case m: CSCMatrix[Double] => m.mapActiveValues(func)
          case _ => throw new IllegalArgumentException("Type " + from.getClass + " is not supported.")
        }
      }
    }
  }
}