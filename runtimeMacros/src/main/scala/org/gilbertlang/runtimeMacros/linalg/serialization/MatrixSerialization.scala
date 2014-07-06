package org.gilbertlang.runtimeMacros.linalg.serialization

import java.io.{DataInput, DataOutput}

import breeze.linalg.{CSCMatrix, DenseMatrix}
import org.gilbertlang.runtimeMacros.linalg.breeze._
import org.gilbertlang.runtimeMacros.linalg.mahout.{MahoutBooleanMatrix, MahoutDoubleMatrix}
import org.gilbertlang.runtimeMacros.linalg.{BooleanMatrix, DoubleMatrix}
import org.apache.mahout.math.{Matrix => MahoutMatrix, DenseMatrix => MahoutDenseMatrix,
SparseMatrix => MahoutSparseMatrix}
import collection.JavaConversions._

object MatrixSerialization {
  val breezeSparseMatrixId = "sparseBreeze"
  val breezeDenseMatrixId = "denseBreeze"
  val breezeBitmatrixId = "bitmatrixBreeze"
  val mahoutSparseMatrixId = "sparseMahout"
  val mahoutDenseMatrixId = "denseMahout"
  val mahoutBooleanSparseMatrixId = "booleanSparseMahout"
  val mahoutBooleanDenseMatrixId = "booleanDenseMahout"
  
  def writeDoubleMatrix(matrix: DoubleMatrix, out: DataOutput) {
    matrix match {
      case m: BreezeDoubleMatrix => writeBreezeDoubleMatrix(matrix, out)
      case m: MahoutDoubleMatrix => writeMahoutDoubleMatrix(matrix, out)
      case _ =>
        throw new IllegalArgumentException("Matrix serialization does not support this matrix type " + matrix.getClass)
    }
  }

  def writeMahoutDoubleMatrix(matrix: MahoutDoubleMatrix, out: DataOutput){
    matrix.matrix match {
      case m: MahoutDenseMatrix =>
        out.writeUTF(this.mahoutDenseMatrixId)
        writeMahoutDenseMatrix(m, out)
      case m: MahoutSparseMatrix =>
        out.writeUTF(this.mahoutSparseMatrixId)
        writeMahoutSparseMatrix(m, out)
      case _ => throw new IllegalArgumentException("Matrix serialization cannot serialize matrix "+ matrix.matrix.getClass)
    }
  }

  def writeMahoutDenseMatrix(matrix: MahoutDenseMatrix, out: DataOutput){
    out.writeInt(matrix.numRows())
    out.writeInt(matrix.numCols())

    for(row <- 0 until matrix.numRows(); col <- 0 until matrix.numCols()){
      out.writeDouble(matrix.getQuick(row, col))
    }
  }

  def writeMahoutSparseMatrix(matrix: MahoutSparseMatrix, out: DataOutput){
    out.writeInt(matrix.numRows())
    out.writeInt(matrix.numCols())

    for(slice <- matrix.iterator()){
      for(element <- slice.nonZeroes()){
        out.writeInt(slice.index())
        out.writeInt(element.index())
        out.writeDouble(element.get())
      }
    }

    out.writeInt(-1)
  }

  def writeBreezeDoubleMatrix(matrix: BreezeDoubleMatrix, out: DataOutput){
    matrix.matrix match {
      case m: DenseMatrix[Double] =>
        out.writeUTF(breezeDenseMatrixId)

        out.writeInt(m.rows)
        out.writeInt(m.cols)

        for(((_), value) <- m.iterator){
          out.writeDouble(value)
        }
      case m: CSCMatrix[Double] =>
        out.writeUTF(breezeSparseMatrixId)

        out.writeInt(m.rows)
        out.writeInt(m.cols)
        out.writeInt(m.activeSize)

        for(((row, col), value) <- m.activeIterator){
          out.writeInt(row)
          out.writeInt(col)
          out.writeDouble(value)
        }
      case _ =>
        throw new IllegalArgumentException("Matrix serialization does not support this breeze double matrix type " +
          matrix.getClass)
    }
  }

  def writeBooleanMatrix(matrix: BooleanMatrix, out: DataOutput) {
    matrix match {
      case m : BreezeBooleanMatrix => writeBreezeBooleanMatrix(m, out)
      case m: MahoutBooleanMatrix => writeMahoutBooleanMatrix(m, out)
      case _ => throw new IllegalArgumentException("Matrix serialization does not support this matrix type " + matrix
        .getClass)
    }
  }

  def writeMahoutBooleanMatrix(matrix: MahoutBooleanMatrix, out: DataOutput){
    matrix.matrix match {
      case m: MahoutDenseMatrix =>
        out.writeUTF(mahoutBooleanDenseMatrixId)
        writeMahoutDenseMatrix(m, out)
      case m: MahoutSparseMatrix =>
        out.writeUTF(mahoutBooleanSparseMatrixId)
        writeMahoutSparseMatrix(m, out)
      case _ =>
        throw new IllegalArgumentException("Matrix serialization cannot serialize matrix of type "+ matrix.matrix
          .getClass)
    }
  }

  def writeBreezeBooleanMatrix(matrix: BreezeBooleanMatrix, out: DataOutput) {
    matrix.matrix match {
      case b: Bitmatrix =>
        out.writeUTF(breezeBitmatrixId)
        out.writeInt(b.rows)
        out.writeInt(b.cols)
        out.writeBoolean(b.isTranspose)
        out.writeInt(b.activeSize)

        for((row, col) <- b.activeKeysIterator){
          out.writeInt(row)
          out.writeInt(col)
        }

      case _ => throw new IllegalArgumentException("Matrix serialization does not support breeze boolean matrix type " +
        matrix.getClass)
    }
  }
  
  def readDoubleMatrix(in: DataInput):DoubleMatrix = {
    val tpeId = in.readUTF()

    tpeId match {
      case this.`breezeDenseMatrixId` => readBreezeDenseMatrix(in)
      case this.`breezeSparseMatrixId` => readBreezeSparseMatrix(in)
      case this.`mahoutDenseMatrixId` => MahoutDoubleMatrix(readMahoutDenseMatrix(in))
      case this.`mahoutSparseMatrixId` => MahoutDoubleMatrix(readMahoutSparseMatrix(in))
      case _ =>
        throw new IllegalArgumentException("Matrix serialization does not support matrix with type id " + tpeId)
    }
  }

  def readMahoutDenseMatrix(in: DataInput): MahoutDenseMatrix = {
    val rows = in.readInt()
    val cols = in.readInt()

    val data = Array.ofDim[Double](rows, cols)

    for(r <- 0 until rows; c <- 0 until cols){
      val value = in.readDouble()
      data(r)(c) = value
    }

    new MahoutDenseMatrix(data, true)
  }

  def readMahoutSparseMatrix(in: DataInput): MahoutSparseMatrix = {
    val rows = in.readInt();
    val cols = in.readInt();

    val matrix = new MahoutSparseMatrix(rows, cols)

    var row: Int  = in.readInt()
    while(row != -1){
      val col = in.readInt()
      val value = in.readDouble()
      matrix.setQuick(row, col, value)
      row = in.readInt()
    }

    matrix
  }

  def readBreezeBitmatrix(in: DataInput): Bitmatrix = {
    val rows = in.readInt()
    val cols = in.readInt()
    val transposed = in.readBoolean()
    val used = in.readInt()
    val result = new Bitmatrix(rows, cols)

    for(_ <- 0 until used){
      val row = in.readInt()
      val col = in.readInt()

      result.update(row, col, value = true)
    }

    result
  }

  def readBooleanMatrix(in: DataInput):BooleanMatrix = {
    val tpeId = in.readUTF()

    tpeId match {
      case this.`breezeBitmatrixId` => readBreezeBitmatrix(in)
      case this.`mahoutBooleanSparseMatrixId` => MahoutBooleanMatrix(readMahoutSparseMatrix(in))
      case this.`mahoutBooleanDenseMatrixId` => MahoutBooleanMatrix(readMahoutDenseMatrix(in))
      case _ =>
        throw new IllegalArgumentException("Matrix serialization does not support matix with type id " + tpeId)
    }
  }

  def readBreezeDenseMatrix(in: DataInput): DenseMatrix[Double] = {
    val rows = in.readInt()
    val cols = in.readInt()

    val result = new DenseMatrix[Double](rows, cols)

    for(row <- 0 until rows; col <- 0 until cols){
      result(row, col) = in.readDouble()
    }

    result
  }

  def readBreezeSparseMatrix(in: DataInput): CSCMatrix[Double] = {
    val rows = in.readInt()
    val cols = in.readInt()
    val activeSize = in.readInt()

    val builder = new CSCMatrix.Builder[Double](rows, cols, activeSize)

    for(_ <- 0 until activeSize){
      val row = in.readInt()
      val col = in.readInt()
      val value = in.readDouble()

      builder.add(row, col, value)
    }

    builder.result()
  }
}