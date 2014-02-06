package org.gilbertlang.runtimeMacros.linalg

import eu.stratosphere.types.Value
import breeze.linalg.{ Matrix => BreezeMatrix, MatrixLike => BreezeMatrixLike, CSCMatrix => BreezeSparseMatrix,
  DenseMatrix => BreezeDenseMatrix, Vector => BreezeVector, DenseVector => BreezeDenseVector,
  SparseVector => BreezeSparseVector}
import java.io.DataOutput
import java.io.DataInput
import breeze.storage.DefaultArrayValue
import org.gilbertlang.runtimeMacros.linalg.operators.GilbertMatrixOps
import breeze.linalg.support.CanTraverseValues
import CanTraverseValues.ValuesVisitor
import breeze.linalg.support.CanMapValues
import breeze.linalg.support.CanZipMapValues
import breeze.linalg.support.CanSlice2
import org.gilbertlang.runtimeMacros.linalg.operators.BreezeMatrixOps
import breeze.linalg.support.CanTranspose
import breeze.linalg.support.CanCollapseAxis
import breeze.linalg.Axis
import breeze.linalg.CSCMatrix

class GilbertMatrix(var matrix: BreezeMatrix[Double]) extends BreezeMatrix[Double] with BreezeMatrixLike[Double, GilbertMatrix] with Value {
  def this() = this(null)

  override def rows = matrix.rows
  override def cols = matrix.cols

  override def activeIterator = matrix.activeIterator
  override def activeSize = matrix.activeSize
  override def activeKeysIterator = matrix.activeKeysIterator
  override def activeValuesIterator = matrix.activeValuesIterator

  override def apply(i: Int, j: Int) = matrix.apply(i, j)

  override def update(i: Int, j: Int, value: Double) = matrix.update(i, j, value)

  override def copy = GilbertMatrix(matrix.copy)

  override def write(out: DataOutput) {
    MatrixSerialization.write(matrix, out)
  }

  override def repr = this

  override def read(in: DataInput) {
    matrix = MatrixSerialization.read(in)
  }
}

object GilbertMatrix extends GilbertMatrixOps with BreezeMatrixOps {
  def apply(matrix: BreezeMatrix[Double]) = new GilbertMatrix(matrix)
  
  def apply(rows: Int, cols: Int, entries: Seq[(Int, Int, Double)]): GilbertMatrix = {
    val size = rows*cols
    val nonZeroElementsRatio = entries.length.toDouble/size
    
    if(nonZeroElementsRatio > Configuration.DENSITYTHRESHOLD){
      val matrix = Configuration.newDenseMatrix(rows,cols)
      for((row, col, value) <- entries ){
        matrix.update(row, col, value)
      }
      GilbertMatrix(matrix)
    }else{
      val builder = new CSCMatrix.Builder[Double](rows, cols, entries.length)
      
      for((row, col, value) <- entries){
        builder.add(row,col,value)
      }
      
      GilbertMatrix(builder.result)
    }
  }
  
  def apply(rows: Int, cols: Int, numNonZeroElements: Int = 0) = {
    val size = rows*cols
    val nonZeroElementsRatio = numNonZeroElements.toDouble/size
    
    if(nonZeroElementsRatio > Configuration.DENSITYTHRESHOLD){
      new GilbertMatrix(Configuration.newDenseMatrix(rows,cols))
    }else{
      new GilbertMatrix(Configuration.newSparseMatrix(rows, cols))
    }
  }
  
  def init(rows: Int, cols: Int, initialValue: Double =  0) = {
    new GilbertMatrix(Configuration.newDenseMatrix(rows, cols, initialValue))
  }
  
  def rand(rows: Int, cols: Int) = {
    GilbertMatrix(BreezeDenseMatrix.rand(rows, cols))
  }
  
  implicit val canMapValues = {
    new CanMapValues[GilbertMatrix, Double, Double, GilbertMatrix]{
      override def map(matrix: GilbertMatrix, fun: Double => Double) = {
        matrix.matrix match {
          case x: Configuration.DenseMatrix => GilbertMatrix(x.map(fun))
          case x: Configuration.SparseMatrix => GilbertMatrix(x.map(fun))
        }
      }
      
      override def mapActive(matrix: GilbertMatrix, fun: Double => Double) = {
        matrix.matrix match{
          case x: Configuration.DenseMatrix => GilbertMatrix(x.mapActiveValues(fun))
          case x: Configuration.SparseMatrix => GilbertMatrix(x.mapActiveValues(fun))
        }
      }
    }
  }
  
  implicit val handHoldCMV = new CanMapValues.HandHold[GilbertMatrix, Double]
  
  implicit val canZipMapValues: CanZipMapValues[GilbertMatrix, Double, Double, GilbertMatrix] = {
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
  
  implicit val canIterateValues: CanTraverseValues[GilbertMatrix, Double] = {
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
          case x: Configuration.DenseMatrix => GilbertMatrix(x(row, ::))
          case x: Configuration.SparseMatrix => GilbertMatrix(x(row, ::))
        }
      }
    }
  }
  
  implicit def canSliceRowsGilbertMatrix: CanSlice2[GilbertMatrix, Range, ::.type, GilbertMatrix] = {
    new CanSlice2[GilbertMatrix, Range, ::.type, GilbertMatrix]{
      override def apply(matrix: GilbertMatrix, rows: Range, ignored: ::.type) = {
        matrix.matrix match{
          case x: Configuration.DenseMatrix => GilbertMatrix(x(rows, ::))
          case x: Configuration.SparseMatrix => GilbertMatrix(x(rows, ::))
        }
      }
    }
  }
  
  implicit def canSliceColGilbertMatrix: CanSlice2[GilbertMatrix, ::.type, Int,  GilbertVector] = {
    new CanSlice2[GilbertMatrix, ::.type, Int , GilbertVector]{
      override def apply(matrix: GilbertMatrix, ignored: ::.type, col: Int) = {
        matrix.matrix match{
          case x: Configuration.DenseMatrix => GilbertVector(x(::, col))
          case x: Configuration.SparseMatrix => GilbertVector(x(::, col))
        }
      }
    }
  }
 
  
  implicit def canSliceColsGilbertMatrix: CanSlice2[GilbertMatrix, ::.type,Range, GilbertMatrix] = {
    new CanSlice2[GilbertMatrix, ::.type, Range, GilbertMatrix]{
      override def apply(matrix: GilbertMatrix, ignored: ::.type, cols: Range) = {
        matrix.matrix match{
          case x: Configuration.DenseMatrix => GilbertMatrix(x(::,cols))
          case x: Configuration.SparseMatrix => GilbertMatrix(x(::, cols))
        }
      }
    }
  }
  
  implicit val canTranspose: CanTranspose[GilbertMatrix, GilbertMatrix] = {
    new CanTranspose[GilbertMatrix, GilbertMatrix]{
      override def apply(gilbertMatrix: GilbertMatrix) = {
        GilbertMatrix(gilbertMatrix.t)
      }
    }
  }
  
  implicit val canCollapseRows: CanCollapseAxis[GilbertMatrix, Axis._0.type, BreezeVector[Double], Double, 
    GilbertMatrix] = new CanCollapseAxis[GilbertMatrix, Axis._0.type, BreezeVector[Double], Double, GilbertMatrix]{
      override def apply(gilbertMatrix: GilbertMatrix, axis: Axis._0.type)(fn: BreezeVector[Double] => Double) = {
        val result = gilbertMatrix.matrix match {
          case x:Configuration.DenseMatrix => 
            val collapser = implicitly[CanCollapseAxis[Configuration.DenseMatrix, Axis._0.type, 
            BreezeDenseVector[Double], Double, Configuration.DenseMatrix]]
            collapser(x, axis)(fn)
          case x: Configuration.SparseMatrix =>
            val collapser = implicitly[CanCollapseAxis[Configuration.SparseMatrix, Axis._0.type,
              BreezeSparseVector[Double], Double, Configuration.SparseMatrix]]
            collapser(x, axis)(fn)
        }
        
        GilbertMatrix(result)
      }
  }
  
  implicit val canCollapseCols: CanCollapseAxis[GilbertMatrix, Axis._1.type, BreezeVector[Double], Double,
    GilbertVector] = new CanCollapseAxis[GilbertMatrix, Axis._1.type, BreezeVector[Double], Double, GilbertVector]{
        override def apply(gilbertMatrix: GilbertMatrix, axis: Axis._1.type)(fn: BreezeVector[Double] => Double) = {
          val result = gilbertMatrix.matrix match {
            case x: Configuration.DenseMatrix =>
              val collapser = implicitly[CanCollapseAxis[Configuration.DenseMatrix, Axis._1.type, 
                BreezeDenseVector[Double], Double, BreezeDenseVector[Double]]]
              collapser(x, axis)(fn)
            case x: Configuration.SparseMatrix =>
              val collapser = implicitly[CanCollapseAxis[Configuration.SparseMatrix, Axis._1.type,
                BreezeSparseVector[Double], Double, BreezeSparseVector[Double]]]
              collapser(x, axis)(fn)
          }
          
          GilbertVector(result)
        }
  }
  
  implicit val handholdCanMapCols: CanCollapseAxis.HandHold[GilbertMatrix, Axis._1.type, BreezeVector[Double]] = 
    new CanCollapseAxis.HandHold[GilbertMatrix, Axis._1.type, BreezeVector[Double]]()
    
  implicit val handholdCanMapRows: CanCollapseAxis.HandHold[GilbertMatrix, Axis._0.type, BreezeVector[Double]] = 
    new CanCollapseAxis.HandHold[GilbertMatrix, Axis._0.type, BreezeVector[Double]]()
}

