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
import breeze.linalg.support.CanCopy
import breeze.math.Semiring
import breeze.storage.DefaultArrayValue
import scala.util.Random
import io.Serializer
import breeze.linalg.Axis
import scala.reflect.ClassTag
import breeze.linalg._
import operators.BreezeMatrixRegistries


final class GilbertMatrix[@specialized(Double, Boolean) T]
(var matrix: BreezeMatrix[T]) extends BreezeMatrix[T] with BreezeMatrixLike[T, GilbertMatrix[T]] with Value {
  def this() = this(null)

  override def rows = matrix.rows
  override def cols = matrix.cols

  override def activeIterator = matrix.activeIterator
  override def activeSize = matrix.activeSize
  override def activeKeysIterator = matrix.activeKeysIterator
  override def activeValuesIterator = matrix.activeValuesIterator

  override def apply(i: Int, j: Int) = matrix.apply(i, j)

  override def update(i: Int, j: Int, value: T) = matrix.update(i, j, value)

  override def copy = GilbertMatrix[T](this.matrix.copy)
  override def write(out: DataOutput){
    MatrixSerialization.write(matrix, out)
  }

  override def repr = this

  override def read(in: DataInput) {
    matrix = MatrixSerialization.read[T](in)
  }
  
  override def toString = {
    matrix.toString
  }
}

object GilbertMatrix extends GilbertMatrixOps with BreezeMatrixOps with BreezeMatrixRegistries {
  def apply[@specialized(Double, Boolean) T]
  (matrix: BreezeMatrix[T]): GilbertMatrix[T] = new GilbertMatrix[T](matrix)
  
  def apply[@specialized(Double, Boolean) T: MatrixFactory](rows: Int, cols: Int, entries: Seq[(Int, Int, T)]): GilbertMatrix[T] = {
    val size = rows*cols
    val nonZeroElementsRatio = entries.length.toDouble/size
    val factory = implicitly[MatrixFactory[T]]
    
    GilbertMatrix(factory.create(rows, cols, entries, nonZeroElementsRatio > Configuration.DENSITYTHRESHOLD))
  }
  
  def apply[@specialized(Double, Boolean) T:MatrixFactory]
  (rows: Int, cols: Int, numNonZeroElements: Int = 0): GilbertMatrix[T] = {
    val size = rows*cols
    val nonZeroElementsRatio = numNonZeroElements.toDouble/size
    val factory = implicitly[MatrixFactory[T]]
    
    GilbertMatrix(factory.create(rows, cols, nonZeroElementsRatio > Configuration.DENSITYTHRESHOLD))
  }
  
  def init[@specialized(Double, Boolean) T:MatrixFactory](rows: Int, cols: Int, initialValue: T) = {
    val factory = implicitly[MatrixFactory[T]]
    GilbertMatrix(factory.create(rows, cols, initialValue, true))
  }
  
  def eye[@specialized(Double, Boolean) T:MatrixFactory](rows: Int, cols: Int) = {
    val size=  rows*cols
    val nonZeroElementsRatio = math.min(rows, cols).toDouble/size
    val factory = implicitly[MatrixFactory[T]]
    
    GilbertMatrix(factory.eye(rows, cols, nonZeroElementsRatio > Configuration.DENSITYTHRESHOLD))
  }
  
  def rand(rows: Int, cols: Int, random: Random = new Random()):GilbertMatrix[Double] = {
    GilbertMatrix(BreezeDenseMatrix.rand(rows, cols, random))
  }
  
  implicit def canCopy[@specialized(Double, Boolean) T:ClassTag:DefaultArrayValue]: CanCopy[GilbertMatrix[T]] =
    new CanCopy[GilbertMatrix[T]]{
    override def apply(gilbert: GilbertMatrix[T]): GilbertMatrix[T] = {
      val internalCopy = gilbert.matrix match {
        case x: BreezeDenseMatrix[T] => breeze.linalg.copy(x)
        case x: BreezeSparseMatrix[T] => breeze.linalg.copy(x)
      }
      new GilbertMatrix[T](internalCopy)
    }
  }
  
  implicit def canMapValues[@specialized(Double, Boolean) T: ClassTag: Semiring: DefaultArrayValue] = {
    new CanMapValues[GilbertMatrix[T], T, T, GilbertMatrix[T]]{
      override def map(matrix: GilbertMatrix[T], fun: T => T) = {
        matrix.matrix match {
          case x: BreezeDenseMatrix[T] => new GilbertMatrix[T](x.map(fun))
          case x: BreezeSparseMatrix[T] => new GilbertMatrix[T](x.map(fun))
        }
      }
      
      override def mapActive(matrix: GilbertMatrix[T], fun: T => T) = {
        matrix.matrix match{
          case x: BreezeDenseMatrix[T] => new GilbertMatrix[T](x.mapActiveValues(fun))
          case x: BreezeSparseMatrix[T] => new GilbertMatrix[T](x.mapActiveValues(fun))
        }
      }
    }
  }
  
  implicit def handHoldCMV[T] = new CanMapValues.HandHold[GilbertMatrix[T], T]
  
  implicit def canZipMapValues[@specialized(Double, Boolean) T:ClassTag:Semiring:DefaultArrayValue]: 
  CanZipMapValues[GilbertMatrix[T], T, T, GilbertMatrix[T]] = {
   new CanZipMapValues[GilbertMatrix[T], T, T, GilbertMatrix[T]]{
     override def map(a: GilbertMatrix[T], b: GilbertMatrix[T], fn: (T, T) => T) = {
       val result = (a.matrix, b.matrix) match {
         case (x:BreezeDenseMatrix[T], y:BreezeDenseMatrix[T]) => 
           val mapper = 
             implicitly[CanZipMapValues[BreezeDenseMatrix[T], T, T, BreezeDenseMatrix[T]]]
           mapper.map(x,y,fn)
         case (x: BreezeSparseMatrix[T], y: BreezeSparseMatrix[T]) =>
           val mapper = 
             implicitly[CanZipMapValues[BreezeSparseMatrix[T], T, T, BreezeSparseMatrix[T]]]
           mapper.map(x,y,fn)
         case _ =>
           val mapper =
             implicitly[CanZipMapValues[BreezeMatrix[T], T, T, BreezeMatrix[T]]]
           mapper.map(a.matrix,b.matrix,fn)
       }
       
       GilbertMatrix(result)
     }
   }
  }
  
  implicit def canIterateValues[T]: CanTraverseValues[GilbertMatrix[T], T] = {
    new CanTraverseValues[GilbertMatrix[T], T]{
      override def isTraversableAgain(gilbertMatrix: GilbertMatrix[T]):Boolean = true
      
      override def traverse(gilbertMatrix: GilbertMatrix[T], fn: ValuesVisitor[T]){
        gilbertMatrix.matrix match {
          case x: BreezeSparseMatrix[T] => { 
            val traversal = implicitly[CanTraverseValues[BreezeSparseMatrix[T], T]]
            traversal.traverse(x, fn)
          }
          case x: BreezeDenseMatrix[T] => {
            val traversal = implicitly[CanTraverseValues[BreezeDenseMatrix[T], T]]
            traversal.traverse(x, fn)
          }
        }
      }
    }
  }
  
   implicit def canSliceRowGilbertMatrix[@specialized(Double, Boolean) T: ClassTag: Semiring: DefaultArrayValue]: 
   CanSlice2[GilbertMatrix[T], Int, ::.type, GilbertMatrix[T]] = {
    new CanSlice2[GilbertMatrix[T], Int, ::.type, GilbertMatrix[T]]{
      override def apply(matrix: GilbertMatrix[T], row: Int, ignored: ::.type) = {
        matrix.matrix match {
          case x: BreezeDenseMatrix[T] => GilbertMatrix(x(row, ::))
          case x: BreezeSparseMatrix[T] => GilbertMatrix(x(row, ::))
        }
      }
    }
  }
  
  implicit def canSliceRowsGilbertMatrix[@specialized(Double, Boolean) T:ClassTag: Semiring: DefaultArrayValue]: 
  CanSlice2[GilbertMatrix[T], Range, ::.type, GilbertMatrix[T]] = {
    new CanSlice2[GilbertMatrix[T], Range, ::.type, GilbertMatrix[T]]{
      override def apply(matrix: GilbertMatrix[T], rows: Range, ignored: ::.type) = {
        matrix.matrix match{
          case x: BreezeDenseMatrix[T] => GilbertMatrix(x(rows, ::))
          case x: BreezeSparseMatrix[T] => GilbertMatrix(x(rows, ::))
        }
      }
    }
  }
  
  implicit def canSliceColGilbertMatrix[@specialized(Double, Boolean) T: ClassTag: Semiring: 
  DefaultArrayValue]: CanSlice2[GilbertMatrix[T], ::.type, Int,  GilbertVector[T]] = {
    new CanSlice2[GilbertMatrix[T], ::.type, Int , GilbertVector[T]]{
      override def apply(matrix: GilbertMatrix[T], ignored: ::.type, col: Int) = {
        matrix.matrix match{
          case x: BreezeDenseMatrix[T] => GilbertVector(x(::, col))
          case x: BreezeSparseMatrix[T] => GilbertVector(x(::, col))
        }
      }
    }
  }
 
  
  implicit def canSliceColsGilbertMatrix[@specialized(Double, Boolean) T: ClassTag: Semiring: 
  DefaultArrayValue]: CanSlice2[GilbertMatrix[T], ::.type,Range, GilbertMatrix[T]] = {
    new CanSlice2[GilbertMatrix[T], ::.type, Range, GilbertMatrix[T]]{
      override def apply(matrix: GilbertMatrix[T], ignored: ::.type, cols: Range) = {
        matrix.matrix match{
          case x: BreezeDenseMatrix[T] => GilbertMatrix(x(::,cols))
          case x: BreezeSparseMatrix[T] => GilbertMatrix(x(::, cols))
        }
      }
    }
  }
  
  implicit def canTranspose[@specialized(Double, Boolean) T: ClassTag: Semiring: 
  DefaultArrayValue]: CanTranspose[GilbertMatrix[T], GilbertMatrix[T]] = {
    new CanTranspose[GilbertMatrix[T], GilbertMatrix[T]]{
      override def apply(gilbertMatrix: GilbertMatrix[T]) = {
        val transposedMatrix = gilbertMatrix.matrix match {
          case x: BreezeDenseMatrix[T] => x.t
          case x: BreezeSparseMatrix[T] => x.t
        }
        GilbertMatrix(transposedMatrix)
      }
    }
  }
  
  implicit def canCollapseRows[@specialized(Double, Boolean) T: ClassTag: Semiring: 
  DefaultArrayValue]: CanCollapseAxis[GilbertMatrix[T], Axis._0.type, BreezeVector[T], T, 
    GilbertMatrix[T]] = new CanCollapseAxis[GilbertMatrix[T], Axis._0.type, BreezeVector[T], T, GilbertMatrix[T]]{
      override def apply(gilbertMatrix: GilbertMatrix[T], axis: Axis._0.type)(fn: BreezeVector[T] => T) = {
        val result = gilbertMatrix.matrix match {
          case x:BreezeDenseMatrix[T] => 
            val collapser = implicitly[CanCollapseAxis[BreezeDenseMatrix[T], Axis._0.type, 
            BreezeDenseVector[T], T, BreezeDenseMatrix[T]]]
            collapser(x, axis)(fn)
          case x: BreezeSparseMatrix[T] =>
            val collapser = implicitly[CanCollapseAxis[BreezeSparseMatrix[T], Axis._0.type,
              BreezeSparseVector[T], T, BreezeSparseMatrix[T]]]
            collapser(x, axis)(fn)
        }
        
        GilbertMatrix(result)
      }
  }
  
  implicit def canCollapseCols[@specialized(Double, Boolean) T: ClassTag: Semiring: 
  DefaultArrayValue]: CanCollapseAxis[GilbertMatrix[T], Axis._1.type, BreezeVector[T], T,
    GilbertVector[T]] = new CanCollapseAxis[GilbertMatrix[T], Axis._1.type, BreezeVector[T], T, GilbertVector[T]]{
        override def apply(gilbertMatrix: GilbertMatrix[T], axis: Axis._1.type)(fn: BreezeVector[T] => T) = {
          val result = gilbertMatrix.matrix match {
            case x: BreezeDenseMatrix[T] =>
              val collapser = implicitly[CanCollapseAxis[BreezeDenseMatrix[T], Axis._1.type, 
                BreezeDenseVector[T], T, BreezeDenseVector[T]]]
              collapser(x, axis)(fn)
            case x: BreezeSparseMatrix[T] =>
              val collapser = implicitly[CanCollapseAxis[BreezeSparseMatrix[T], Axis._1.type,
                BreezeSparseVector[T], T, BreezeSparseVector[T]]]
              collapser(x, axis)(fn)
          }
          
          GilbertVector(result)
        }
  }
  
  
  implicit def handholdCanMapCols[T]: CanCollapseAxis.HandHold[GilbertMatrix[T], Axis._1.type, BreezeVector[T]] = 
    new CanCollapseAxis.HandHold[GilbertMatrix[T], Axis._1.type, BreezeVector[T]]()
    
  implicit def handholdCanMapRows[T]: CanCollapseAxis.HandHold[GilbertMatrix[T], Axis._0.type, BreezeVector[T]] = 
    new CanCollapseAxis.HandHold[GilbertMatrix[T], Axis._0.type, BreezeVector[T]]()
}

