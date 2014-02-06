package org.gilbertlang.runtimeMacros.linalg

import breeze.linalg.{Matrix => BreezeMatrix, CSCMatrix => BreezeSparseMatrix, DenseMatrix => BreezeDenseMatrix}
import java.io.DataOutput
import java.io.DataInput
import scala.reflect.runtime.universe._
import breeze.linalg.{CSCMatrix => BreezeSparseMatrix}
import breeze.linalg.{DenseMatrix => BreezeDenseMatrix}
import breeze.linalg.{Matrix => BreezeMatrix}

object MatrixSerialization {
  val sparseMatrixId = "SparseMatrix"
  val denseMatrixId = "DenseMatrix"
  
  def write(matrix: BreezeMatrix[Double], out: DataOutput) {
    matrix match {
      case x: Configuration.SparseMatrix => writeBreezeSparseMatrix(x, out)
      case x: Configuration.DenseMatrix => writeBreezeDenseMatrix(x, out)
    }
  }
  
  def read(in: DataInput): Configuration.Matrix = {
    val id = in.readUTF()
    id match {
      case `sparseMatrixId` => readBreezeSparseMatrix(in)
      case `denseMatrixId` => readBreezeDenseMatrix(in)
    }
  }
  
  def writeBreezeSparseMatrix(matrix: BreezeSparseMatrix[Double], out: DataOutput) {
    out.writeUTF(sparseMatrixId)
    out.writeInt(matrix.rows)
    out.writeInt(matrix.cols)
    out.writeInt(matrix.activeSize)
    
    for(((row, column), value) <- matrix.activeIterator){
      out.writeInt(row)
      out.writeInt(column)
      out.writeDouble(value)
    } 
  }
  
  def readBreezeSparseMatrix(in: DataInput): BreezeSparseMatrix[Double] = {
    val rows = in.readInt()
    val cols = in.readInt()
    val nnzs = in.readInt()
    val builder = new BreezeSparseMatrix.Builder[Double](rows, cols, nnzs)
    
    for(_ <- 0 until nnzs){
      val r = in.readInt()
      val c = in.readInt()
      val v = in.readDouble()
      builder.add(r, c, v)
    }
    
    builder.result
  }
  
  def writeBreezeDenseMatrix(matrix: BreezeDenseMatrix[Double], out: DataOutput){
    out.writeUTF(denseMatrixId)
    out.writeInt(matrix.rows)
    out.writeInt(matrix.cols)
    
    for(((_,_),value) <- matrix.iterator){
      out.writeDouble(value)
    }
  }
  
  def readBreezeDenseMatrix(in: DataInput): BreezeDenseMatrix[Double] = {
    val rows = in.readInt()
    val cols = in.readInt()
    
    val result = new BreezeDenseMatrix[Double](rows, cols)
    
    for(i <- Iterator.range(0,rows); j <- Iterator.range(0,cols)){
      val v = in.readDouble()
      result(i,j) = v
    }
    
    result
  }
}