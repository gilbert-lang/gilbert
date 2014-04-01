package org.gilbertlang.runtimeMacros.linalg

import eu.stratosphere.types.Value
import breeze.linalg.{ Matrix => BreezeMatrix, MatrixLike => BreezeMatrixLike, CSCMatrix => BreezeSparseMatrix,
  DenseMatrix => BreezeDenseMatrix, Vector => BreezeVector, DenseVector => BreezeDenseVector,
  SparseVector => BreezeSparseVector}
import java.io.DataOutput
import java.io.DataInput
import breeze.storage.DefaultArrayValue
import breeze.linalg.support.CanTraverseValues
import CanTraverseValues.ValuesVisitor
import breeze.linalg.support.CanMapValues
import breeze.linalg.support.CanZipMapValues
import breeze.linalg.support.CanSlice2
import org.gilbertlang.runtimeMacros.linalg.operators.BreezeMatrixOps
import breeze.linalg.support.CanTranspose
import breeze.linalg.support.CanCollapseAxis
import breeze.linalg.support.CanCopy
import breeze.math.Semiring
import breeze.storage.DefaultArrayValue
import scala.util.Random
import io.Serializer
import breeze.linalg.Axis
import scala.reflect.ClassTag
import breeze.linalg._
import operators.BreezeMatrixRegistries
import org.gilbertlang.runtimeMacros.linalg.operators.GilbertMatrixBooleanOps


final class GilbertMatrixBoolean(var matrix: Bitmatrix) extends BreezeMatrix[Boolean] with BreezeMatrixLike[Boolean, 
  GilbertMatrixBoolean] with Value {
  def this() = this(null)

  override def rows = matrix.rows
  override def cols = matrix.cols

  override def activeIterator = matrix.activeIterator
  override def activeSize = matrix.activeSize
  override def activeKeysIterator = matrix.activeKeysIterator
  override def activeValuesIterator = matrix.activeValuesIterator

  override def apply(i: Int, j: Int) = matrix.apply(i, j)

  override def update(i: Int, j: Int, value: Boolean) = matrix.update(i, j, value)

  override def copy = GilbertMatrixBoolean(this.matrix.copy)
  
  override def write(out: DataOutput){
    MatrixSerialization.writeBitMatrix(matrix, out)
  }

  override def repr = this

  override def read(in: DataInput) {
    matrix = MatrixSerialization.readBitMatrix(in)
  }
  
  override def toString() = {
    matrix.toString
  }
}

object GilbertMatrixBoolean extends GilbertMatrixBooleanOps{
  def apply(matrix: Bitmatrix): GilbertMatrixBoolean = new GilbertMatrixBoolean(matrix)
  
  def apply(rows: Int, cols: Int, entries: Seq[(Int, Int, Boolean)]): GilbertMatrixBoolean= {
    val factory = MatrixFactory.BooleanFactory
    GilbertMatrixBoolean(factory.create(rows, cols, entries, dense = false))
  }
  
  def apply(rows: Int, cols: Int, numNonZeroElements: Int = 0): GilbertMatrixBoolean = {
    val factory = MatrixFactory.BooleanFactory
    GilbertMatrixBoolean(factory.create(rows, cols, dense = false))
  }
  
  def init(rows: Int, cols: Int, initialValue: Boolean) = {
    val factory = MatrixFactory.BooleanFactory
    GilbertMatrixBoolean(factory.init(rows, cols, initialValue, dense = false))
  }
  
  def eye(rows: Int, cols: Int) = {
    val factory = MatrixFactory.BooleanFactory
    GilbertMatrixBoolean(factory.eye(rows, cols,dense = false))
  }
  
  implicit def canCopy: CanCopy[GilbertMatrixBoolean] =
    new CanCopy[GilbertMatrixBoolean]{
    override def apply(gilbert: GilbertMatrixBoolean): GilbertMatrixBoolean = {
      GilbertMatrixBoolean(gilbert.matrix.copy)
    }
  }
  
  implicit def canTranspose: CanTranspose[GilbertMatrixBoolean, GilbertMatrixBoolean] = {
    new CanTranspose[GilbertMatrixBoolean, GilbertMatrixBoolean]{
      override def apply(gilbert: GilbertMatrixBoolean) = {
        GilbertMatrixBoolean(gilbert.matrix.t)
      }
    }
  }
}

