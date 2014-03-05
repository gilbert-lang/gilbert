package org.gilbertlang.runtimeMacros.linalg

import eu.stratosphere.types.Value
import breeze.linalg.{ Matrix => BreezeMatrix, MatrixLike => BreezeMatrixLike, CSCMatrix => BreezeSparseMatrix,
  DenseMatrix => BreezeDenseMatrix, Vector => BreezeVector, DenseVector => BreezeDenseVector,
  SparseVector => BreezeSparseVector}
import java.io.DataOutput
import java.io.DataInput
import org.gilbertlang.runtimeMacros.linalg.operators.GilbertMatrixOps
import breeze.linalg.support.CanTraverseValues
import CanTraverseValues.ValuesVisitor
import breeze.linalg.support.CanMapValues
import breeze.linalg.support.CanZipMapValues
import breeze.linalg.support.CanSlice2
import org.gilbertlang.runtimeMacros.linalg.operators.BreezeMatrixOps
import breeze.linalg.support.CanTranspose
import breeze.linalg.support.CanCollapseAxis
import breeze.linalg.support.CanCopy
import scala.util.Random
import breeze.linalg._
import operators.BreezeMatrixRegistries


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
  
  override def toString = {
    matrix.toString
  }
}

object GilbertMatrix extends GilbertMatrixOps with BreezeMatrixOps with BreezeMatrixRegistries {
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
    GilbertMatrix(factory.init(rows, cols, initialValue, true))
  }
  
  def eye(rows: Int, cols: Int) = {
    val size=  rows*cols
    val nonZeroElementsRatio = math.min(rows, cols).toDouble/size
    val factory = implicitly[MatrixFactory[Double]]
    
    GilbertMatrix(factory.eye(rows, cols, nonZeroElementsRatio > Configuration.DENSITYTHRESHOLD))
  }
  
  def rand(rows: Int, cols: Int, random: Random = new Random()):GilbertMatrix = {
    GilbertMatrix(BreezeDenseMatrix.rand(rows, cols, random))
  }
  
  implicit def canCopy: CanCopy[GilbertMatrix] =
    new CanCopy[GilbertMatrix]{
    override def apply(gilbert: GilbertMatrix): GilbertMatrix = {
      val internalCopy = gilbert.matrix match {
        case x: BreezeDenseMatrix[Double] => breeze.linalg.copy(x)
        case x: BreezeSparseMatrix[Double] => breeze.linalg.copy(x)
      }
      GilbertMatrix(internalCopy)
    }
  }
  
  implicit def canMapValues = {
    new CanMapValues[GilbertMatrix, Double, Double, GilbertMatrix]{
      override def map(matrix: GilbertMatrix, fun: Double => Double) = {
        matrix.matrix match {
          case x: BreezeDenseMatrix[Double] => GilbertMatrix(x.map(fun))
          case x: BreezeSparseMatrix[Double] => GilbertMatrix(x.map(fun))
        }
      }
      
      override def mapActive(matrix: GilbertMatrix, fun: Double => Double) = {
        matrix.matrix match{
          case x: BreezeDenseMatrix[Double] => GilbertMatrix(x.mapActiveValues(fun))
          case x: BreezeSparseMatrix[Double] => GilbertMatrix(x.mapActiveValues(fun))
        }
      }
    }
  }
  
  implicit def handHoldCMV = new CanMapValues.HandHold[GilbertMatrix, Double]
  
  implicit def canZipMapValues: CanZipMapValues[GilbertMatrix, Double, Double, GilbertMatrix] = {
   new CanZipMapValues[GilbertMatrix, Double, Double, GilbertMatrix]{
     override def map(a: GilbertMatrix, b: GilbertMatrix, fn: (Double, Double) => Double) = {
       val result = (a.matrix, b.matrix) match {
         case (x:BreezeDenseMatrix[Double], y:BreezeDenseMatrix[Double]) => 
           val mapper = 
             implicitly[CanZipMapValues[BreezeDenseMatrix[Double], Double, Double, BreezeDenseMatrix[Double]]]
           mapper.map(x,y,fn)
         case (x: BreezeSparseMatrix[Double], y: BreezeSparseMatrix[Double]) =>
           val mapper = 
             implicitly[CanZipMapValues[BreezeSparseMatrix[Double], Double, Double, BreezeSparseMatrix[Double]]]
           mapper.map(x,y,fn)
         case _ =>
           val mapper =
             implicitly[CanZipMapValues[BreezeMatrix[Double], Double, Double, BreezeMatrix[Double]]]
           mapper.map(a.matrix,b.matrix,fn)
       }
       
       GilbertMatrix(result)
     }
   }
  }
  
  implicit def canIterateValues: CanTraverseValues[GilbertMatrix, Double] = {
    new CanTraverseValues[GilbertMatrix, Double]{
      override def isTraversableAgain(gilbertMatrix: GilbertMatrix):Boolean = true
      
      override def traverse(gilbertMatrix: GilbertMatrix, fn: ValuesVisitor[Double]){
        gilbertMatrix.matrix match {
          case x: BreezeSparseMatrix[Double] => { 
            val traversal = implicitly[CanTraverseValues[BreezeSparseMatrix[Double], Double]]
            traversal.traverse(x, fn)
          }
          case x: BreezeDenseMatrix[Double] => {
            val traversal = implicitly[CanTraverseValues[BreezeDenseMatrix[Double], Double]]
            traversal.traverse(x, fn)
          }
        }
      }
    }
  }
  
   implicit def canSliceRowGilbertMatrix: CanSlice2[GilbertMatrix, Int, ::.type, GilbertMatrix] = {
    new CanSlice2[GilbertMatrix, Int, ::.type, GilbertMatrix]{
      override def apply(matrix: GilbertMatrix, row: Int, ignored: ::.type) = {
        matrix.matrix match {
          case x: BreezeDenseMatrix[Double] => GilbertMatrix(x(row, ::))
          case x: BreezeSparseMatrix[Double] => GilbertMatrix(x(row, ::))
        }
      }
    }
  }
  
  implicit def canSliceRowsGilbertMatrix: CanSlice2[GilbertMatrix, Range, ::.type, GilbertMatrix] = {
    new CanSlice2[GilbertMatrix, Range, ::.type, GilbertMatrix]{
      override def apply(matrix: GilbertMatrix, rows: Range, ignored: ::.type) = {
        matrix.matrix match{
          case x: BreezeDenseMatrix[Double] => GilbertMatrix(x(rows, ::))
          case x: BreezeSparseMatrix[Double] => GilbertMatrix(x(rows, ::))
        }
      }
    }
  }
  
  implicit def canSliceColGilbertMatrix: CanSlice2[GilbertMatrix, ::.type, Int,  GilbertVector] = {
    new CanSlice2[GilbertMatrix, ::.type, Int , GilbertVector]{
      override def apply(matrix: GilbertMatrix, ignored: ::.type, col: Int) = {
        matrix.matrix match{
          case x: BreezeDenseMatrix[Double] => GilbertVector(x(::, col))
          case x: BreezeSparseMatrix[Double] => GilbertVector(x(::, col))
        }
      }
    }
  }
 
  
  implicit def canSliceColsGilbertMatrix: CanSlice2[GilbertMatrix, ::.type,Range, GilbertMatrix] = {
    new CanSlice2[GilbertMatrix, ::.type, Range, GilbertMatrix]{
      override def apply(matrix: GilbertMatrix, ignored: ::.type, cols: Range) = {
        matrix.matrix match{
          case x: BreezeDenseMatrix[Double] => GilbertMatrix(x(::,cols))
          case x: BreezeSparseMatrix[Double] => GilbertMatrix(x(::, cols))
        }
      }
    }
  }
  
  implicit def canTranspose: CanTranspose[GilbertMatrix, GilbertMatrix] = {
    new CanTranspose[GilbertMatrix, GilbertMatrix]{
      override def apply(gilbertMatrix: GilbertMatrix) = {
        val transposedMatrix = gilbertMatrix.matrix match {
          case x: BreezeDenseMatrix[Double] => x.t
          case x: BreezeSparseMatrix[Double] => x.t
        }
        GilbertMatrix(transposedMatrix)
      }
    }
  }
  
  implicit def canCollapseRows: CanCollapseAxis[GilbertMatrix, Axis._0.type, BreezeVector[Double], Double, 
    GilbertMatrix] = new CanCollapseAxis[GilbertMatrix, Axis._0.type, BreezeVector[Double], Double, GilbertMatrix]{
      override def apply(gilbertMatrix: GilbertMatrix, axis: Axis._0.type)(fn: BreezeVector[Double] => Double) = {
        val result = gilbertMatrix.matrix match {
          case x:BreezeDenseMatrix[Double] => 
            val collapser = implicitly[CanCollapseAxis[BreezeDenseMatrix[Double], Axis._0.type, 
            BreezeDenseVector[Double], Double, BreezeDenseMatrix[Double]]]
            collapser(x, axis)(fn)
          case x: BreezeSparseMatrix[Double] =>
            val collapser = implicitly[CanCollapseAxis[BreezeSparseMatrix[Double], Axis._0.type,
              BreezeSparseVector[Double], Double, BreezeSparseMatrix[Double]]]
            collapser(x, axis)(fn)
        }
        
        GilbertMatrix(result)
      }
  }
  
  implicit def canCollapseCols: CanCollapseAxis[GilbertMatrix, Axis._1.type, BreezeVector[Double], Double,
    GilbertVector] = new CanCollapseAxis[GilbertMatrix, Axis._1.type, BreezeVector[Double], Double, GilbertVector]{
        override def apply(gilbertMatrix: GilbertMatrix, axis: Axis._1.type)(fn: BreezeVector[Double] => Double) = {
          val result = gilbertMatrix.matrix match {
            case x: BreezeDenseMatrix[Double] =>
              val collapser = implicitly[CanCollapseAxis[BreezeDenseMatrix[Double], Axis._1.type, 
                BreezeDenseVector[Double], Double, BreezeDenseVector[Double]]]
              collapser(x, axis)(fn)
            case x: BreezeSparseMatrix[Double] =>
              val collapser = implicitly[CanCollapseAxis[BreezeSparseMatrix[Double], Axis._1.type,
                BreezeSparseVector[Double], Double, BreezeSparseVector[Double]]]
              collapser(x, axis)(fn)
          }
          
          GilbertVector(result)
        }
  }
  
  
  implicit def handholdCanMapCols: CanCollapseAxis.HandHold[GilbertMatrix, Axis._1.type, BreezeVector[Double]] = 
    new CanCollapseAxis.HandHold[GilbertMatrix, Axis._1.type, BreezeVector[Double]]()
    
  implicit def handholdCanMapRows: CanCollapseAxis.HandHold[GilbertMatrix, Axis._0.type, BreezeVector[Double]] = 
    new CanCollapseAxis.HandHold[GilbertMatrix, Axis._0.type, BreezeVector[Double]]()
}

