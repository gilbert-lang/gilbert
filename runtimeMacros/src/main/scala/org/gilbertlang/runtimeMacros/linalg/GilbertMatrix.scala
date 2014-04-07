package org.gilbertlang.runtimeMacros.linalg

import eu.stratosphere.types.Value
import breeze.linalg.{ Matrix => BreezeMatrix, MatrixLike => BreezeMatrixLike, CSCMatrix => BreezeSparseMatrix,
  DenseMatrix => BreezeDenseMatrix, Vector => BreezeVector, DenseVector => BreezeDenseVector,
  SparseVector => BreezeSparseVector}
import java.io.DataOutput
import java.io.DataInput
import org.gilbertlang.runtimeMacros.linalg.operators._
import breeze.linalg.support.CanTraverseValues
import CanTraverseValues.ValuesVisitor
import breeze.linalg.support.CanMapValues
import breeze.linalg.support.CanZipMapValues
import breeze.linalg.support.CanSlice2
import breeze.linalg.support.CanTranspose
import breeze.linalg.support.CanCollapseAxis
import breeze.linalg.support.CanCopy
import scala.util.Random
import breeze.linalg._


final class GilbertMatrix(var matrix: BreezeMatrix[Double]) extends BreezeMatrix[Double] with BreezeMatrixLike[Double,
  GilbertMatrix] with Value {
  def this() = this(null)

  override def rows = matrix.rows
  override def cols = matrix.cols

  override def activeIterator = matrix.activeIterator
  override def activeSize = matrix.activeSize
  override def activeKeysIterator = matrix.activeKeysIterator
  override def activeValuesIterator = matrix.activeValuesIterator

  override def apply(i: Int, j: Int) = matrix.apply(i, j)

  override def update(i: Int, j: Int, value: Double) = matrix.update(i, j, value)

  override def copy = GilbertMatrix(this.matrix.copy)
  
  override def write(out: DataOutput){
    MatrixSerialization.write(matrix, out)
  }

  override def repr = this

  override def read(in: DataInput) {
    matrix = MatrixSerialization.read(in)
  }
  
  override def toString() = {
    matrix.toString()
  }
}

object GilbertMatrix extends GilbertMatrixOps with BreezeMatrixOps with BreezeMatrixImplicits {
  def apply(matrix: BreezeMatrix[Double]): GilbertMatrix = new GilbertMatrix(matrix)
  
  def apply(rows: Int, cols: Int, entries: Seq[(Int, Int, Double)]): GilbertMatrix= {
    val size = rows*cols
    val nonZeroElementsRatio = entries.length.toDouble/size
    val factory = implicitly[MatrixFactory[Double]]
    
    GilbertMatrix(factory.create(rows, cols, entries, nonZeroElementsRatio > Configuration.DENSITYTHRESHOLD))
  }
  
  def apply(rows: Int, cols: Int, numNonZeroElements: Int = 0): GilbertMatrix = {
    val size = rows*cols
    val nonZeroElementsRatio = numNonZeroElements.toDouble/size
    val factory = implicitly[MatrixFactory[Double]]
    
    GilbertMatrix(factory.create(rows, cols, nonZeroElementsRatio > Configuration.DENSITYTHRESHOLD))
  }
  
  def init(rows: Int, cols: Int, initialValue: Double) = {
    val factory = implicitly[MatrixFactory[Double]]
    GilbertMatrix(factory.init(rows, cols, initialValue, dense = true))
  }
  
  def eye(rows: Int, cols: Int) = {
    val size=  rows*cols
    val nonZeroElementsRatio = math.min(rows, cols).toDouble/size
    val factory = implicitly[MatrixFactory[Double]]
    
    GilbertMatrix(factory.eye(rows, cols, nonZeroElementsRatio > Configuration.DENSITYTHRESHOLD))
  }

  def eye(rows: Int, cols: Int, startRow: Int, startCol: Int) = {
    val size = rows*cols
    val nonZeroElementsRatio = math.min(rows-startRow, cols-startCol).toDouble/size
    val factory = implicitly[MatrixFactory[Double]]

    GilbertMatrix(factory.eye(rows, cols, startRow, startCol, nonZeroElementsRatio > Configuration.DENSITYTHRESHOLD))
  }
  
  def rand(rows: Int, cols: Int, random: Random = new Random()):GilbertMatrix = {
    GilbertMatrix(BreezeDenseMatrix.rand(rows, cols, random))
  }
  
  implicit def canCopy: CanCopy[GilbertMatrix] =
    new CanCopy[GilbertMatrix]{
    override def apply(gilbert: GilbertMatrix): GilbertMatrix = {
      val internalCopy = breeze.linalg.copy(gilbert.matrix)
      GilbertMatrix(internalCopy)
    }
  }
  
  implicit def canMapValues = {
    new CanMapValues[GilbertMatrix, Double, Double, GilbertMatrix]{
      override def map(matrix: GilbertMatrix, fun: Double => Double) = {
        GilbertMatrix(matrix.matrix.map(fun))
      }
      
      override def mapActive(matrix: GilbertMatrix, fun: Double => Double) = {
        GilbertMatrix(matrix.matrix.mapActiveValues(fun))
      }
    }
  }
  
  implicit def handHoldCMV = new CanMapValues.HandHold[GilbertMatrix, Double]
  
  implicit def canZipMapValues: CanZipMapValues[GilbertMatrix, Double, Double, GilbertMatrix] = {
   new CanZipMapValues[GilbertMatrix, Double, Double, GilbertMatrix]{
     override def map(a: GilbertMatrix, b: GilbertMatrix, fn: (Double, Double) => Double) = {
       val mapper = implicitly[CanZipMapValues[BreezeMatrix[Double], Double, Double, BreezeMatrix[Double]]]
       val result = mapper.map(a,b,fn)
       
       GilbertMatrix(result)
     }
   }
  }
  
  implicit def canIterateValues: CanTraverseValues[GilbertMatrix, Double] = {
    new CanTraverseValues[GilbertMatrix, Double]{
      override def isTraversableAgain(gilbertMatrix: GilbertMatrix):Boolean = true
      
      override def traverse(gilbertMatrix: GilbertMatrix, fn: ValuesVisitor[Double]){
        val traversal = implicitly[CanTraverseValues[BreezeMatrix[Double], Double]]
        traversal.traverse(gilbertMatrix.matrix, fn)
      }
    }
  }
  
   implicit def canSliceRowGilbertMatrix: CanSlice2[GilbertMatrix, Int, ::.type, GilbertMatrix] = {
    new CanSlice2[GilbertMatrix, Int, ::.type, GilbertMatrix]{
      override def apply(matrix: GilbertMatrix, row: Int, ignored: ::.type) = {
        GilbertMatrix(matrix.matrix(row, ::))
      }
    }
  }
  
  implicit def canSliceRowsGilbertMatrix: CanSlice2[GilbertMatrix, Range, ::.type, GilbertMatrix] = {
    new CanSlice2[GilbertMatrix, Range, ::.type, GilbertMatrix]{
      override def apply(matrix: GilbertMatrix, rows: Range, ignored: ::.type) = {
        GilbertMatrix(matrix.matrix(rows, ::))
      }
    }
  }
  
  implicit def canSliceColGilbertMatrix: CanSlice2[GilbertMatrix, ::.type, Int,  GilbertVector] = {
    new CanSlice2[GilbertMatrix, ::.type, Int , GilbertVector]{
      override def apply(matrix: GilbertMatrix, ignored: ::.type, col: Int) = {
        GilbertVector(matrix.matrix(::, col))
      }
    }
  }
 
  
  implicit def canSliceColsGilbertMatrix: CanSlice2[GilbertMatrix, ::.type,Range, GilbertMatrix] = {
    new CanSlice2[GilbertMatrix, ::.type, Range, GilbertMatrix]{
      override def apply(matrix: GilbertMatrix, ignored: ::.type, cols: Range) = {
        GilbertMatrix(matrix.matrix(::, cols))
      }
    }
  }
  
  implicit def canTranspose: CanTranspose[GilbertMatrix, GilbertMatrix] = {
    new CanTranspose[GilbertMatrix, GilbertMatrix]{
      override def apply(gilbertMatrix: GilbertMatrix) = {
        GilbertMatrix(gilbertMatrix.matrix.t)
      }
    }
  }
  
  implicit def canCollapseRows: CanCollapseAxis[GilbertMatrix, Axis._0.type, BreezeVector[Double], Double, 
    GilbertMatrix] = new CanCollapseAxis[GilbertMatrix, Axis._0.type, BreezeVector[Double], Double, GilbertMatrix]{
      override def apply(gilbertMatrix: GilbertMatrix, axis: Axis._0.type)(fn: BreezeVector[Double] => Double) = {
        val collapser = implicitly[CanCollapseAxis[BreezeMatrix[Double], Axis._0.type, BreezeVector[Double], Double,
          BreezeMatrix[Double]]]
        val result = collapser(gilbertMatrix.matrix, axis)(fn)
        GilbertMatrix(result)
      }
  }
  
  implicit def canCollapseCols: CanCollapseAxis[GilbertMatrix, Axis._1.type, BreezeVector[Double], Double,
    GilbertVector] = new CanCollapseAxis[GilbertMatrix, Axis._1.type, BreezeVector[Double], Double, GilbertVector]{
        override def apply(gilbertMatrix: GilbertMatrix, axis: Axis._1.type)(fn: BreezeVector[Double] => Double) = {
          val collapser = implicitly[CanCollapseAxis[BreezeMatrix[Double], Axis._1.type, BreezeVector[Double],
            Double, BreezeVector[Double]]]
          val result = collapser(gilbertMatrix.matrix,axis)(fn)
          GilbertVector(result)
        }
  }
  
  
  implicit def handholdCanMapCols: CanCollapseAxis.HandHold[GilbertMatrix, Axis._1.type, BreezeVector[Double]] = 
    new CanCollapseAxis.HandHold[GilbertMatrix, Axis._1.type, BreezeVector[Double]]()
    
  implicit def handholdCanMapRows: CanCollapseAxis.HandHold[GilbertMatrix, Axis._0.type, BreezeVector[Double]] = 
    new CanCollapseAxis.HandHold[GilbertMatrix, Axis._0.type, BreezeVector[Double]]()
}

