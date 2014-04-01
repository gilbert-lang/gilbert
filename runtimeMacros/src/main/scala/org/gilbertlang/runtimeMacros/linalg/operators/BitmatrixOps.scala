package org.gilbertlang.runtimeMacros.linalg.operators

import org.gilbertlang.runtimeMacros.linalg.Bitmatrix
import breeze.linalg.operators.OpSet
import breeze.linalg.operators.OpAnd
import breeze.linalg.operators.OpOr
import breeze.linalg.operators.BinaryUpdateRegistry
import breeze.linalg.operators.BinaryRegistry
import breeze.macros.expand
import breeze.linalg.operators.OpType
import breeze.linalg._

trait BitmatrixOps {
  this: Bitmatrix.type =>
  
  implicit object SetBMBMOp extends OpSet.InPlaceImpl2[Bitmatrix, Bitmatrix]{
    def apply(a: Bitmatrix, b: Bitmatrix) {
      require(a.rows == b.rows, "Matrixs must have same number of rows")
      require(a.cols == b.cols, " Matrixs must have same number of columns")
      
      for(c <- 0 until a.cols; r <- 0 until a.rows){
        a.update(r,c,b(r,c))
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
          a.update(row, col, value = false)
        }
      }
    }
  }

  implicit val andBMSUpdateOp: BinaryUpdateRegistry[Bitmatrix, Boolean, OpAnd.type] = 
  new BinaryUpdateRegistry[Bitmatrix, Boolean, OpAnd.type]{
    override def bindingMissing(a: Bitmatrix, b: Boolean) {
      if(!b){
        a.data.set(0, a.size, false)
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
          a.update(row, col, value = true)
        }
      }
    }
  }

  implicit val orBMSUpdateOp: BinaryUpdateRegistry[Bitmatrix, Boolean, OpOr.type] =
  new BinaryUpdateRegistry[Bitmatrix, Boolean, OpOr.type]{
    override def bindingMissing(a: Bitmatrix, b: Boolean) {
      if(b){
        a.data.set(0, a.size, true)
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

  implicit object AndBMSOp extends OpAnd.Impl2[Bitmatrix, Boolean, Bitmatrix]{
    override def apply(a: Bitmatrix, b: Boolean): Bitmatrix = {
      val result = copy(a)
      result &= b
      result
    }
    implicitly[BinaryRegistry[Matrix[Boolean], Boolean, OpAnd.type, Matrix[Boolean]]].register(this)
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

  implicit object OrBMSOp extends OpOr.Impl2[Bitmatrix, Boolean, Bitmatrix]{
    override def apply(a: Bitmatrix, b: Boolean): Bitmatrix = {
      val result = copy(a)
      result |= b
      result
    }
    implicitly[BinaryRegistry[Matrix[Boolean], Boolean, OpOr.type, Matrix[Boolean]]].register(this)
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

  implicit val AndBMSOpInPlace: OpAnd.InPlaceImpl2[Bitmatrix, Boolean] = 
  new OpAnd.InPlaceImpl2[Bitmatrix, Boolean]{
    override def apply(a: Bitmatrix, b: Boolean) {
      if(!b){
        a.data.set(0, a.size, false)
      }
    }
    implicitly[BinaryUpdateRegistry[Matrix[Boolean], Boolean, OpAnd.type]].register(this)
    implicitly[BinaryUpdateRegistry[Bitmatrix, Boolean, OpAnd.type]].register(this)

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

  implicit val OrBMSOpInPlae: OpOr.InPlaceImpl2[Bitmatrix, Boolean] = new OpOr.InPlaceImpl2[Bitmatrix, Boolean]{
    override def apply(a: Bitmatrix, b: Boolean) {
      if(b){
          a.data.set(0, a.size, true)
        }
    }

    implicitly[BinaryUpdateRegistry[Matrix[Boolean], Boolean, OpOr.type]].register(this)
    implicitly[BinaryUpdateRegistry[Bitmatrix, Boolean, OpOr.type]].register(this)
  
  }
}