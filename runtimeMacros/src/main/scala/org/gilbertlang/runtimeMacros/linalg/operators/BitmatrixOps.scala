package org.gilbertlang.runtimeMacros.linalg.operators

import org.gilbertlang.runtimeMacros.linalg.Bitmatrix
import breeze.linalg.operators.OpSet
import breeze.linalg.operators.OpAnd
import breeze.linalg.operators.OpOr
import breeze.linalg.operators.BinaryUpdateRegistry
import breeze.linalg.Matrix
import breeze.linalg.operators.BinaryRegistry
import breeze.macros.expand
import breeze.linalg.operators.OpType
import breeze.linalg._
import breeze.linalg.support.CanCopy

trait BitmatrixOps {
  
  implicit val canCopy: CanCopy[Bitmatrix] = new CanCopy[Bitmatrix]{
    override def apply(a: Bitmatrix) = {
      a.copy
    }
  }
  
  implicit object SetBMBMOp extends OpSet.InPlaceImpl2[Bitmatrix, Bitmatrix]{
    def apply(a: Bitmatrix, b: Bitmatrix) {
      require(a.rows == b.rows, "Matrixs must have same number of rows")
      require(a.cols == b.cols, " Matrixs must have same number of columns")
      
      for(c <- 0 until a.cols; r <- 0 until a.rows){
        a.update(r,c,b(r,c))
      }
    }
  }
  
  @expand
  @expand.valify
  implicit def op_MMUpdate[@expand.args(OpOr, OpAnd) Op <: OpType](implicit 
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
  
  implicit val andBMMUpdateOp: BinaryUpdateRegistry[Bitmatrix, Matrix[Boolean], OpAnd.type] =
    new BinaryUpdateRegistry[Bitmatrix, Matrix[Boolean], OpAnd.type] {
    override def bindingMissing(a: Bitmatrix, b: Matrix[Boolean]){
      require(a.rows == b.rows, "Matrixs must have the same number of rows")
      require(a.cols == b.cols, "Matrixs must have the same number of cols")
      
      for((row, col) <- a.activeKeysIterator){
        if(!b(row,col)){
          a.update(row, col, false)
        }
      }
    }
  }
  
  implicit val orBMMUpdateOp: BinaryUpdateRegistry[Bitmatrix, Matrix[Boolean], OpOr.type] = 
  new BinaryUpdateRegistry[Bitmatrix, Matrix[Boolean], OpOr.type]{
    override def bindingMissing(a: Bitmatrix, b:Matrix[Boolean]){
       require(a.rows == b.rows, "Matrixs must have the same number of rows")
      require(a.cols == b.cols, "Matrixs must have the same number of cols")
      
      for(col <- 0 until a.cols; row <- 0 until a.rows){
        if(b(row,col)){
          a.update(row, col, true)
        }
      }
    }
  }
  
  @expand
  @expand.valify
  implicit def op_MM[@expand.args(OpOr, OpAnd) Op <: OpType]: 
    BinaryRegistry[Matrix[Boolean], Matrix[Boolean], Op.type, Matrix[Boolean]] = {
    val uop = implicitly[Op.InPlaceImpl2[Matrix[Boolean], Matrix[Boolean]]]
    new BinaryRegistry[Matrix[Boolean], Matrix[Boolean], Op.type, Matrix[Boolean]]{
      override def bindingMissing(a: Matrix[Boolean], b: Matrix[Boolean]): Matrix[Boolean] = {
        val c = copy(a)
        uop(c,b)
        c
      }
    }
  }
  
  @expand
  @expand.valify
  implicit def op_BMM[@expand.args(OpOr, OpAnd) Op <: OpType]: BinaryRegistry[Bitmatrix, Matrix[Boolean], Op.type, Bitmatrix] =
    new BinaryRegistry[Bitmatrix, Matrix[Boolean], Op.type, Bitmatrix] {
    val uop = implicitly[BinaryUpdateRegistry[Bitmatrix, Matrix[Boolean], Op.type]]
    override def bindingMissing(a: Bitmatrix, b: Matrix[Boolean]): Bitmatrix = {
      val c = copy(a)
      uop(c,b)
      c
    }
  }
  
  implicit object AndBMBMOp extends OpAnd.Impl2[Bitmatrix, Bitmatrix, Bitmatrix]{
    override def apply(a: Bitmatrix, b: Bitmatrix): Bitmatrix = {
      val result = copy(a)
      result &= b
      result
    }
    
    implicitly[BinaryRegistry[Matrix[Boolean], Matrix[Boolean], OpAnd.type, Matrix[Boolean]]].register(this)
    implicitly[BinaryRegistry[Bitmatrix, Matrix[Boolean], OpAnd.type, Bitmatrix]].register(this)
  }
  
  implicit object OrBMBMOp extends OpOr.Impl2[Bitmatrix, Bitmatrix, Bitmatrix]{
    override def apply(a: Bitmatrix, b: Bitmatrix): Bitmatrix = {
      val result = copy(a)
      result |= b
      result
    }
    
    
    implicitly[BinaryRegistry[Matrix[Boolean], Matrix[Boolean], OpOr.type, Matrix[Boolean]]].register(this)
    implicitly[BinaryRegistry[Bitmatrix, Matrix[Boolean], OpOr.type, Bitmatrix]].register(this)
  }
  
  implicit val AndBMBMOpInPlace:OpAnd.InPlaceImpl2[Bitmatrix, Bitmatrix] = 
    new OpAnd.InPlaceImpl2[Bitmatrix, Bitmatrix]{
    override def apply(a: Bitmatrix, b: Bitmatrix) {
      require(a.rows == b.rows, "Matrixs must have the same number of rows")
      require(a.cols == b.cols, "Matrixs must have the same number of cols")
      
      val bData = 
      if(a.isTranspose != b.isTranspose){
        val transposedData = new java.util.BitSet(b.data.size())
        for( (row, col) <- b.activeKeysIterator){
          transposedData.set(a.linearIndex(row,col))
        }
        transposedData
      }else{
        b.data
      }
      
      a.data.intersects(bData)
    }
    
    implicitly[BinaryUpdateRegistry[Matrix[Boolean], Matrix[Boolean], OpAnd.type]].register(this)
    implicitly[BinaryUpdateRegistry[Bitmatrix, Matrix[Boolean], OpAnd.type]].register(this)
  }
  
  implicit val OrBMBMOpInPlace: OpOr.InPlaceImpl2[Bitmatrix, Bitmatrix] = new OpOr.InPlaceImpl2[Bitmatrix, Bitmatrix]{
    override def apply(a: Bitmatrix, b: Bitmatrix) {
      require(a.rows == b.rows, "Matrixs must have the same number of rows")
      require(a.cols == b.cols, "Matrixs must have the same number of cols")
      
      val bData = 
      if(a.isTranspose != b.isTranspose){
       val transposedData = new java.util.BitSet(b.data.size())
        for( (row, col) <- b.activeKeysIterator){
          transposedData.set(a.linearIndex(row,col))
        }
        transposedData
      }else{
        b.data
      }
      
      a.data.or(bData)
    }
    
    implicitly[BinaryUpdateRegistry[Matrix[Boolean], Matrix[Boolean], OpOr.type]].register(this)
    implicitly[BinaryUpdateRegistry[Bitmatrix, Matrix[Boolean], OpOr.type]].register(this)
  
  }
}