package org.gilbertlang.runtimeMacros.linalg.serialization

import java.io.{DataInput, DataOutput}

import breeze.linalg.{CSCMatrix, DenseMatrix}
import org.gilbertlang.runtimeMacros.linalg.breeze._
import org.gilbertlang.runtimeMacros.linalg.{BooleanMatrix, DoubleMatrix}

object MatrixSerialization {
  val sparseBreezeMatrixId = "sparseBreeze"
  val denseBreezeMatrixId = "denseBreeze"
  val bitmatrixBreezeId = "bitmatrixBreeze"
  val sparseMahoutMatrixId = "sparseMahout"
  val denseMahoutMatrixId = "denseMahout"
  val booleanMahoutMatrixIdx = "booleanMahout"
  
  def writeDoubleMatrix(matrix: DoubleMatrix, out: DataOutput) {
    matrix match {
      case m: BreezeDoubleMatrix => writeBreezeDoubleMatrix(matrix, out)
      case _ =>
        throw new IllegalArgumentException("Matrix serialization does not support this matrix type " + matrix.getClass)
    }
  }

  def writeBreezeDoubleMatrix(matrix: BreezeDoubleMatrix, out: DataOutput){
    matrix.matrix match {
      case m: DenseMatrix[Double] =>
        out.writeUTF(denseBreezeMatrixId)

        out.writeInt(m.rows)
        out.writeInt(m.cols)

        for(((_), value) <- m.iterator){
          out.writeDouble(value)
        }
      case m: CSCMatrix[Double] =>
        out.writeUTF(sparseBreezeMatrixId)

        out.writeInt(m.rows)
        out.writeInt(m.cols)
        out.writeInt(m.activeSize)

        for(((row, col), value) <- m.iterator){
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
      case _ => throw new IllegalArgumentException("Matrix serialization does not support this matrix type " + matrix
        .getClass)
    }
  }

  def writeBreezeBooleanMatrix(matrix: BreezeBooleanMatrix, out: DataOutput) {
    matrix.matrix match {
      case b: Bitmatrix =>
        out.writeUTF(bitmatrixBreezeId)
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
      case this.denseBreezeMatrixId => readBreezeDenseMatrix(in)
      case this.sparseBreezeMatrixId => readBreezeSparseMatrix(in)
      case _ =>
        throw new IllegalArgumentException("Matrix serialization does not support matrix with type id " + tpeId)
    }
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
      case this.bitmatrixBreezeId => readBreezeBitmatrix(in)
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