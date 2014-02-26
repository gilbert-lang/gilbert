package org.gilbertlang.runtimeMacros.linalg

import breeze.linalg.{Matrix => BreezeMatrix, CSCMatrix => BreezeSparseMatrix, DenseMatrix => BreezeDenseMatrix}
import java.io.DataOutput
import java.io.DataInput
import scala.reflect.runtime.universe._
import io.Serializer
import scala.reflect.ClassTag
import breeze.math.Semiring
import breeze.storage.DefaultArrayValue

object MatrixSerialization {
  val sparseMatrixId = "SparseMatrix"
  val denseMatrixId = "DenseMatrix"
  val bitMatrixId = "BitMatrix"
  
  def write[@specialized(Double, Boolean) T: Serializer](matrix: BreezeMatrix[T], out: DataOutput) {
    matrix match {
      case x: BreezeSparseMatrix[T] => 
        out.writeUTF(sparseMatrixId)
        writeBreezeSparseMatrix(x, out)
      case x: BreezeDenseMatrix[T] => 
        out.writeUTF(denseMatrixId)
        writeBreezeDenseMatrix(x, out)
    }
  }
  
  def read[@specialized(Double, Boolean) T: Serializer:ClassTag:DefaultArrayValue:Semiring](in: DataInput): 
  BreezeMatrix[T] = {
    val id = in.readUTF()
    id match {
      case `sparseMatrixId` => readBreezeSparseMatrix[T](in)
      case `denseMatrixId` => readBreezeDenseMatrix[T](in)
    }
  }
  
  
  def writeBreezeSparseMatrix[@specialized(Double, Boolean) T: Serializer](matrix: BreezeSparseMatrix[T], out: DataOutput) {
    out.writeInt(matrix.rows)
    out.writeInt(matrix.cols)
    out.writeInt(matrix.activeSize)
    val writer = implicitly[Serializer[T]]
    
    for(((row, column), value) <- matrix.activeIterator){
      out.writeInt(row)
      out.writeInt(column)
      writer.write(value, out)
    } 
  }
  
  def readBreezeSparseMatrix[@specialized(Double, Boolean) T:Serializer:ClassTag:Semiring:DefaultArrayValue]
  (in: DataInput): BreezeSparseMatrix[T] = {
    val rows = in.readInt()
    val cols = in.readInt()
    val nnzs = in.readInt()
    val reader = implicitly[Serializer[T]]
        
    val builder = new BreezeSparseMatrix.Builder[T](rows, cols, nnzs)
    
    for(_ <- 0 until nnzs){
      val r = in.readInt()
      val c = in.readInt()
      val v = reader.read(in)
      builder.add(r, c, v)
    }
    
    builder.result
  }
  
  def writeBreezeDenseMatrix[@specialized(Double, Boolean) T: Serializer](matrix: BreezeDenseMatrix[T], out: DataOutput){
    out.writeInt(matrix.rows)
    out.writeInt(matrix.cols)
    val writer = implicitly[Serializer[T]]
    
    for(((_,_),value) <- matrix.iterator){
      writer.write(value,out)
    }
  }
  
  def readBreezeDenseMatrix[@specialized(Double, Boolean) T:Serializer:ClassTag](in: DataInput): BreezeDenseMatrix[T] = {
    val rows = in.readInt()
    val cols = in.readInt()
    val reader = implicitly[Serializer[T]]
    
    val result = new BreezeDenseMatrix[T](rows, cols)
    
    for(i <- Iterator.range(0,rows); j <- Iterator.range(0,cols)){
      val v = reader.read(in)
      result(i,j) = v
    }
    
    result
  }
  
  def writeBitMatrix(matrix: Bitmatrix, out: DataOutput){
    out.writeInt(matrix.rows)
    out.writeInt(matrix.cols)
    out.writeBoolean(matrix.isTranspose)
    out.writeInt(matrix.activeSize)
    
    for((row, col) <- matrix.activeKeysIterator){
      out.writeInt(row)
      out.writeInt(col)
    }
  }
  
  def readBitMatrix(in: DataInput): Bitmatrix = {
    val rows = in.readInt()
    val cols = in.readInt()
    val transposed = in.readBoolean()
    val used = in.readInt()
    val result = new Bitmatrix(rows, cols)
    
    for(_ <- 0 until used){
      val row = in.readInt()
      val col = in.readInt()
      
      result.update(row, col, true)
    }
    
    result
  }
}