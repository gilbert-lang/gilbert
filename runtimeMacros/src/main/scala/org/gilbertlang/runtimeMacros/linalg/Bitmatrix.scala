package org.gilbertlang.runtimeMacros.linalg

import breeze.linalg.Matrix
import breeze.linalg.MatrixLike
import org.gilbertlang.runtimeMacros.linalg.operators.BitmatrixOps
import org.gilbertlang.runtimeMacros.linalg.operators.BreezeMatrixOps
import org.gilbertlang.runtimeMacros.linalg.operators.BreezeMatrixRegistries
import breeze.linalg.support.CanCopy
import breeze.linalg.support.CanTranspose

class Bitmatrix(val rows: Int, val cols: Int, val data: java.util.BitSet,
    val isTranspose: Boolean) 
  extends Matrix[Boolean] with MatrixLike[Boolean, Bitmatrix] with Serializable {

  def this(rows: Int, cols: Int) = this(rows, cols, 
      new java.util.BitSet(rows*cols), false)
  
  
  def apply(row: Int, col: Int) = {
    if(row < 0 || row >= rows) throw new IndexOutOfBoundsException((row, col) + 
        s" not in [0, $rows) x [0, $cols)")
    if(col < 0 || col >= cols) throw new IndexOutOfBoundsException((row, col) + 
        s" not in [0, $rows) x [0, $cols)")
    
    data.get(linearIndex(row, col))
  }
  
  def linearIndex(row: Int, col: Int) = {
    if(isTranspose)
      col+ row*cols
    else
      row + col*rows;
  }
  
  def rowColIndex(linearIndex: Int) = {
    if(isTranspose){
      (linearIndex / cols, linearIndex % cols)
    }else
      (linearIndex % rows, linearIndex / rows)
  }
  
  def update(row: Int, col: Int, value: Boolean){
     if(row < 0 || row >= rows) throw new IndexOutOfBoundsException((row, col) + 
        s" not in [0, $rows) x [0, $cols)")
    if(col < 0 || col >= cols) throw new IndexOutOfBoundsException((row, col) + 
        s" not in [0, $rows) x [0, $cols)")
    
    data.set(linearIndex(row, col), value)
  }
  
  def copy: Bitmatrix = {
    new Bitmatrix(rows, cols, this.data.clone().asInstanceOf[java.util.BitSet], 
        isTranspose)
  }
  
  def repr = this
  
  def activeSize: Int = data.cardinality()
  
  def activeKeysIterator: Iterator[(Int, Int)] = {
    val firstBit = data.nextSetBit(0)
    if(firstBit < 0) return Iterator.empty
    
    new Iterator[(Int, Int)] {
      var nextReady = true
      var nextResult = firstBit
      
      def hasNext: Boolean = (nextResult >= 0) && (nextReady ||
          {
            nextResult += 1
            nextResult = data.nextSetBit(nextResult)
            nextReady = nextResult >= 0
            nextReady
          })
          
      def next(): (Int, Int) = {
        if(!nextReady) {
          hasNext
          if(!nextReady) throw new NoSuchElementException
        }
        nextReady = false
        rowColIndex(nextResult)
      }
    }
  }
  
  def activeIterator: Iterator[((Int, Int), Boolean)] = activeKeysIterator.map(_ -> true)
  def activeValuesIterator: Iterator[Boolean] = activeKeysIterator.map(_ => true)
  
  override def toString = {
    activeKeysIterator.mkString("Bitmatrix(", ", ", ")")
  }
}

object Bitmatrix extends BitmatrixOps with BreezeMatrixRegistries{
  def zeros(rows: Int, cols: Int): Bitmatrix = {
    new Bitmatrix(rows, cols)
  }
  
  def init(rows: Int, cols: Int, initialValue: Boolean): Bitmatrix = {
    val data = new java.util.BitSet(rows*cols)
    data.set(0,rows*cols, initialValue)
    new Bitmatrix(rows, cols, data, false)
  }
  
  implicit val canTranspose = new CanTranspose[Bitmatrix, Bitmatrix]{
    override def apply(bitmatrix: Bitmatrix): Bitmatrix = {
      new Bitmatrix(bitmatrix.cols, bitmatrix.rows, bitmatrix.data,!bitmatrix.isTranspose)
    }
  }
  
  implicit val canCopy = new CanCopy[Bitmatrix]{
    override def apply(bitmatrix: Bitmatrix): Bitmatrix = {
      bitmatrix.copy
    }
  }
}