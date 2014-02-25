package org.gilbertlang.runtimeMacros.linalg

import breeze.linalg.{Matrix => BreezeMatrix, CSCMatrix => BreezeSparseMatrix, DenseMatrix => BreezeDenseMatrix}
import java.io.DataOutput
import java.io.DataInput
import scala.reflect.runtime.universe._
import io.DataWriter
import io.DataReader
import scala.reflect.ClassTag
import breeze.math.Semiring
import breeze.storage.DefaultArrayValue

object MatrixSerialization {
  val sparseMatrixId = "SparseMatrix"
  val denseMatrixId = "DenseMatrix"
  
  def write[@specialized(Double, Boolean) T: DataWriter](matrix: BreezeMatrix[T], out: DataOutput) {
    matrix match {
      case x: BreezeSparseMatrix[T] => writeBreezeSparseMatrix(x, out)
      case x: BreezeDenseMatrix[T] => writeBreezeDenseMatrix(x, out)
    }
  }
  
  def read[@specialized(Double, Boolean) T: ClassTag: DataReader: Semiring: DefaultArrayValue](in: DataInput): BreezeMatrix[T] = {
    val id = in.readUTF()
    id match {
      case `sparseMatrixId` => readBreezeSparseMatrix[T](in)
      case `denseMatrixId` => readBreezeDenseMatrix[T](in)
    }
  }
  
  
  def writeBreezeSparseMatrix[@specialized(Double, Boolean) T: DataWriter](matrix: BreezeSparseMatrix[T], out: DataOutput) {
    out.writeUTF(sparseMatrixId)
    out.writeInt(matrix.rows)
    out.writeInt(matrix.cols)
    out.writeInt(matrix.activeSize)
    val writer = implicitly[DataWriter[T]]
    
    for(((row, column), value) <- matrix.activeIterator){
      out.writeInt(row)
      out.writeInt(column)
      writer.write(value, out)
    } 
  }
  
  def readBreezeSparseMatrix[@specialized(Double, Boolean) T: ClassTag: DataReader: Semiring: DefaultArrayValue](in: DataInput): BreezeSparseMatrix[T] = {
    val rows = in.readInt()
    val cols = in.readInt()
    val nnzs = in.readInt()
    val builder = new BreezeSparseMatrix.Builder[T](rows, cols, nnzs)
    val reader = implicitly[DataReader[T]]
    for(_ <- 0 until nnzs){
      val r = in.readInt()
      val c = in.readInt()
      val v = reader.read(in)
      builder.add(r, c, v)
    }
    
    builder.result
  }
  
  def writeBreezeDenseMatrix[@specialized(Double, Boolean) T: DataWriter](matrix: BreezeDenseMatrix[T], out: DataOutput){
    out.writeUTF(denseMatrixId)
    out.writeInt(matrix.rows)
    out.writeInt(matrix.cols)
    val writer = implicitly[DataWriter[T]]
    
    for(((_,_),value) <- matrix.iterator){
      writer.write(value,out)
    }
  }
  
  def readBreezeDenseMatrix[@specialized(Double, Boolean) T:ClassTag: DataReader](in: DataInput): BreezeDenseMatrix[T] = {
    val rows = in.readInt()
    val cols = in.readInt()
    
    val result = new BreezeDenseMatrix[T](rows, cols)
    val reader = implicitly[DataReader[T]]
    
    for(i <- Iterator.range(0,rows); j <- Iterator.range(0,cols)){
      val v = reader.read(in)
      result(i,j) = v
    }
    
    result
  }
}