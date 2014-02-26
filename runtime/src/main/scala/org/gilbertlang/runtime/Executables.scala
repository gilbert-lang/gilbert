package org.gilbertlang.runtime

import Operations._

object Executables {

  sealed trait Executable {
    val id: Int = IDGenerator.nextID()

    def instantiate(args: Executable*): Executable
  }
  
  object Executable{
    var instantiated: Boolean = false
  }

  case class CompoundExecutable(executables: List[Executable]) extends Executable {
    def instantiate(args: Executable*): CompoundExecutable = {   
      var anyInstantiated = false
      val instantiatedExecutables = executables map { exec => 
        val instantiatedExec = exec.instantiate(args: _*)
        anyInstantiated |= Executable.instantiated 
        instantiatedExec}
      
      if(anyInstantiated){
        Executable.instantiated = anyInstantiated
        CompoundExecutable(instantiatedExecutables)
      }else
        this
    }
  }
  
  case class WriteMatrix(matrix: Matrix) extends Executable {
    def instantiate(args: Executable*): WriteMatrix = {
      val instantiatedMatrix = matrix.instantiate(args: _*)
      
      if(Executable.instantiated)
        WriteMatrix(instantiatedMatrix)
      else
        this
    }
  }
  case class WriteScalarRef(scalar: ScalarRef) extends Executable {
    def instantiate(args: Executable*): WriteScalarRef = {
      val instantiatedScalar = scalar.instantiate(args: _*)
      
      if(Executable.instantiated)
        WriteScalarRef(instantiatedScalar)
      else
        this
    }
  }

  case class WriteString(string: StringRef) extends Executable {
    def instantiate(args: Executable*): WriteString = {
      val instantiatedString = string.instantiate(args: _*)
      
      if(Executable.instantiated)
        WriteString(instantiatedString)
      else 
        this
    }
  }

  case class WriteFunction(function: FunctionRef) extends Executable {
    def instantiate(args: Executable*): WriteFunction = {
      val instantiatedFunction = function.instantiate(args: _*)
      
      if(Executable.instantiated)
        WriteFunction(instantiatedFunction)
      else
        this
    }
  }

  sealed trait ExpressionExecutable extends Executable

  case object VoidExecutable extends ExpressionExecutable {
    def instantiate(args: Executable*) = {
      Executable.instantiated = false
      VoidExecutable
    }
  }

  sealed trait Matrix extends ExpressionExecutable {
    val rows: Option[Int]
    val cols: Option[Int]

    def transpose() = { Transpose(this) }

    def times(other: Matrix) = { MatrixMult(this, other) }
    def times(scalar: ScalarRef) = { ScalarMatrixTransformation(scalar, this, Multiplication) }

    def div(scalar: ScalarRef) = { ScalarMatrixTransformation(scalar, this, Division) }
    def plus(other: Matrix) = { CellwiseMatrixMatrixTransformation(this, other, Addition) }

    def binarize() = { CellwiseMatrixTransformation(this, Binarize) }

    def max() = { AggregateMatrixTransformation(this, Maximum) }
    def norm(p: Int) = {
      p match {
        case 2 => AggregateMatrixTransformation(this, Norm2)
      }
    }

    def t() = transpose
    def *(other: Matrix) = times(other)
    def *(scalar: ScalarRef) = times(scalar)
    def +(other: Matrix) = plus(other)

    def /(scalar: ScalarRef) = div(scalar)

    def normalizeRows(norm: Int) = {
      norm match {
        case 1 => VectorwiseMatrixTransformation(this, NormalizeL1)
      }
    }

    def instantiate(args: Executable*): Matrix
  }

  sealed abstract class BinaryMatrixTransformation extends Matrix
  sealed abstract class UnaryMatrixTransformation extends Matrix
  sealed abstract class FunctionMatrixTransformation extends Matrix

  case class ScalarMatrixTransformation(scalar: ScalarRef, matrix: Matrix, operation: ScalarMatrixOperation)
    extends BinaryMatrixTransformation {
    val rows = matrix.rows
    val cols = matrix.cols

    def instantiate(args: Executable*): ScalarMatrixTransformation = {
      var anyInstantiated = false
      val instantiatedScalar = scalar.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedMatrix = matrix.instantiate(args:_*)
      anyInstantiated |= Executable.instantiated
      
      if(anyInstantiated){
        Executable.instantiated = true
        ScalarMatrixTransformation(instantiatedScalar, instantiatedMatrix, operation)
      }else
        this
    }
  }
  case class MatrixScalarTransformation(matrix: Matrix, scalar: ScalarRef, operation: ScalarMatrixOperation)
    extends BinaryMatrixTransformation {
    val rows = matrix.rows
    val cols = matrix.cols

    def instantiate(args: Executable*): MatrixScalarTransformation = {
      var anyInstantiated = false
      val instantiatedMatrix = matrix.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedScalar = scalar.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      
      if(anyInstantiated){
        Executable.instantiated = true
        MatrixScalarTransformation(instantiatedMatrix, instantiatedScalar, operation)
      }else
        this
    }
  }
  case class MatrixMult(left: Matrix, right: Matrix) extends BinaryMatrixTransformation {
    val rows = left.rows
    val cols = right.cols

    def instantiate(args: Executable*): MatrixMult = {
      MatrixMult(left.instantiate(args: _*), right.instantiate(args: _*))
    }
  }
  case class CellwiseMatrixMatrixTransformation(left: Matrix, right: Matrix, operation: CellwiseOperation)
    extends BinaryMatrixTransformation {
    val rows = left.rows
    val cols = right.cols

    def instantiate(args: Executable*): CellwiseMatrixMatrixTransformation = {
      var anyInstantiated = false
      val instantiatedLeft = left.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedRight = right.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      
      if(anyInstantiated){
        Executable.instantiated = true
        CellwiseMatrixMatrixTransformation(instantiatedLeft, instantiatedRight, operation)
      }else
        this
    }
  }

  case class Transpose(matrix: Matrix) extends UnaryMatrixTransformation {
    val rows = matrix.cols
    val cols = matrix.rows

    def instantiate(args: Executable*): Transpose = {
      val instantiatedMatrix = matrix.instantiate(args:_*)
      
      if(Executable.instantiated)
        Transpose(instantiatedMatrix)
      else
        this
    }
  }
  case class CellwiseMatrixTransformation(matrix: Matrix, operation: UnaryScalarOperation)
    extends UnaryMatrixTransformation {
    val rows = matrix.rows
    val cols = matrix.cols

    def instantiate(args: Executable*): CellwiseMatrixTransformation = {
      val instantiatedMatrix = matrix.instantiate(args: _*)
      
      if(Executable.instantiated)
        CellwiseMatrixTransformation(instantiatedMatrix, operation)
      else
        this
    }
  }
  case class VectorwiseMatrixTransformation(matrix: Matrix, operation: VectorwiseOperation)
    extends UnaryMatrixTransformation {
    val (rows, cols) = getMatrixSize

    def getMatrixSize: (Option[Int], Option[Int]) = {
      operation match {
        case NormalizeL1 => (matrix.rows, matrix.cols)
        case Minimum | Maximum | Norm2 => {
          if (matrix.cols == Some(1)) {
            (Some(1), Some(1))
          } else {
            (matrix.rows, Some(1))
          }
        }
      }
    }

    def instantiate(args: Executable*): VectorwiseMatrixTransformation = {
      val instantiatedMatrix = matrix.instantiate(args: _*)
      
      if(Executable.instantiated)
        VectorwiseMatrixTransformation(instantiatedMatrix, operation)
      else
        this
    }
  }

  case class LoadMatrix(path: StringRef, numRows: ScalarRef, numColumns: ScalarRef) extends FunctionMatrixTransformation {
    val rows = scalarRef2Int(numRows)
    val cols = scalarRef2Int(numColumns)

    def instantiate(args: Executable*): LoadMatrix = {
      var anyInstantiated = false
      val instantiatedPath = path.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedRows = numRows.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedCols = numColumns.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      
      if(anyInstantiated){
        Executable.instantiated = true
        LoadMatrix(instantiatedPath, instantiatedRows, instantiatedCols)
      }else{
        this
      }
    }
  }
  case class ones(numRows: ScalarRef, numColumns: ScalarRef) extends FunctionMatrixTransformation {
    val rows = scalarRef2Int(numRows)
    val cols = scalarRef2Int(numColumns)

    def instantiate(args: Executable*): ones = {
      var anyInstantiated = false
      val instantiatedRows = numRows.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedCols = numColumns.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      
      if(anyInstantiated){
        Executable.instantiated = true
        ones(instantiatedRows, instantiatedCols)
      }else
        this
    }
  }
  case class randn(numRows: ScalarRef, numColumns: ScalarRef, mean: ScalarRef = scalar(0), std: ScalarRef = scalar(1))
    extends FunctionMatrixTransformation {
    val rows = scalarRef2Int(numRows)
    val cols = scalarRef2Int(numColumns)

    def instantiate(args: Executable*): randn = {
      var anyInstantiated = false
      val instantiatedRows = numRows.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedCols = numColumns.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedMean = mean.instantiate(args:_*)
      anyInstantiated |= Executable.instantiated
      val instantiatedStd = std.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      
      if(anyInstantiated){
        Executable.instantiated = true
        randn(instantiatedRows, instantiatedCols, instantiatedMean, instantiatedStd)
      }else
        this
    }
  }

  case class spones(matrix: Matrix) extends FunctionMatrixTransformation {
    val rows = matrix.rows
    val cols = matrix.cols

    def instantiate(args: Executable*): spones = {
      val instantiatedMatrix = matrix.instantiate(args: _*)
      
      if(Executable.instantiated)
        spones(instantiatedMatrix)
      else
        this
    }
  }

  //TODO remove
  case class sum(matrix: Matrix, dimension: ScalarRef) extends FunctionMatrixTransformation {
    val rows = getRows
    val cols = getCols

    def getRows: Option[Int] = {
      dimension match {
        case scalar(1) => Some(1)
        case scalar(2) => matrix.rows
        case _ => None
      }
    }

    def getCols: Option[Int] = {
      dimension match {
        case scalar(1) => matrix.cols
        case scalar(2) => Some(1)
        case _ => None
      }
    }

    def instantiate(args: Executable*): sum = {
      var anyInstantiated = false
      val instantiatedMatrix = matrix.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedDimension = dimension.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      
      if(anyInstantiated){
        Executable.instantiated = true
        sum(instantiatedMatrix, instantiatedDimension)
      }else
        this
    }
  }

  case class sumRow(matrix: Matrix) extends FunctionMatrixTransformation {
    val rows = matrix.rows
    val cols = Some(1)

    def instantiate(args: Executable*): sumRow = {
      val instantiatedMatrix = matrix.instantiate(args: _*)
      
      if(Executable.instantiated)
        sumRow(instantiatedMatrix)
      else
        this
    }
  }

  case class sumCol(matrix: Matrix) extends FunctionMatrixTransformation {
    val rows = Some(1)
    val cols = matrix.cols

    def instantiate(args: Executable*): sumCol = {
      val instantiatedMatrix = matrix.instantiate(args: _*)
      
      if(Executable.instantiated)
        sumCol(instantiatedMatrix)
      else
        this
    }
  }

  case class diag(matrix: Matrix) extends FunctionMatrixTransformation {
    val (rows, cols) = getRowsCols

    def instantiate(args: Executable*): diag = {
      val instantiatedMatrix = matrix.instantiate(args:_*)
      
      if(Executable.instantiated)
        diag(instantiatedMatrix)
      else
        this
    }

    def getRowsCols: (Option[Int], Option[Int]) = {
      (matrix.rows, matrix.cols) match {
        case (None, _) => (None, None)
        case (_, None) => (None, None)
        case (Some(1), y) => (y, y)
        case (x, Some(1)) => (x, x)
        case (Some(x), Some(y)) => {
          val result = Some(math.min(x, y))
          (result, Some(1))
        }
      }
    }
  }
  
  case class zeros(numRows: ScalarRef, numCols: ScalarRef) extends FunctionMatrixTransformation {
    val rows = scalarRef2Int(numRows)
    val cols = scalarRef2Int(numCols)
    
    def instantiate(args: Executable*): zeros = {
      var anyInstantiated = false
      val instantiatedRows = numRows.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedCols = numCols.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      
      if(anyInstantiated){
        Executable.instantiated = true
        zeros(instantiatedRows, instantiatedCols)
      }else
        this
    }
  }
  
  case class eye(numRows: ScalarRef, numCols: ScalarRef) extends FunctionMatrixTransformation {
    val rows = scalarRef2Int(numRows)
    val cols = scalarRef2Int(numCols)
    
    def instantiate(args: Executable*): eye = {
      var anyInstantiated = false
      val instantiatedRows = numRows.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedCols = numCols.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      
      if(anyInstantiated){
        eye(instantiatedRows, instantiatedCols)
      }else
        this
    }
  }

  case class FixpointIteration(initialState: Matrix, updateFunction: FunctionRef, maxIterations: ScalarRef) extends 
  Matrix {
    val updatePlan = updateFunction.apply(IterationStatePlaceholder)

    val rows = initialState.rows
    val cols = initialState.cols

    def instantiate(args: Executable*): FixpointIteration = {
      var anyInstantiated = false
      val instantiatedState = initialState.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedUpdateFunction = updateFunction.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedMaxIterations = maxIterations.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      
      if(anyInstantiated){
        Executable.instantiated = true
        FixpointIteration(instantiatedState, instantiatedUpdateFunction, instantiatedMaxIterations)
      }else
        this
    }
  }
  case object IterationStatePlaceholder extends Matrix {
    val rows = None
    val cols = None

    def instantiate(args: Executable*): Matrix = {
      Executable.instantiated = false
      IterationStatePlaceholder
    }
  }

  sealed trait StringRef extends ExpressionExecutable {
    val length: Int
    val value: String

    def instantiate(args: Executable*): StringRef = {
      Executable.instantiated = false
      this
    }
  }

  case class string(value: String) extends StringRef {
    val length = value.length
  }

  sealed trait ScalarRef extends ExpressionExecutable {

    def *(matrix: Matrix) = { ScalarMatrixTransformation(this, matrix, Multiplication) }
    def /(matrix: Matrix) = { ScalarMatrixTransformation(this, matrix, Division) }
    def /(other: ScalarRef) = { ScalarScalarTransformation(this, other, Division) }
    def -(other: ScalarRef) = { ScalarScalarTransformation(this, other, Subtraction) }

    def instantiate(args: Executable*): ScalarRef
  }

  case class scalar(value: Double) extends ScalarRef {
    def instantiate(args: Executable*): scalar = {
      Executable.instantiated = false
      this
    }
  }

  case class AggregateMatrixTransformation(matrix: Matrix, operation: AggregateMatrixOperation) extends ScalarRef {
    def instantiate(args: Executable*): AggregateMatrixTransformation = {
      val instantiatedMatrix = matrix.instantiate(args: _*)
      
      if(Executable.instantiated)
        AggregateMatrixTransformation(instantiatedMatrix, operation)
      else
        this
    }
  }

  case class ScalarScalarTransformation(left: ScalarRef, right: ScalarRef, operation: ScalarsOperation) extends ScalarRef {
    def instantiate(args: Executable*): ScalarScalarTransformation = {
      var anyInstantiated = false
      val instantiatedLeft = left.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedRight = right.instantiate(args:_*)
      anyInstantiated |= Executable.instantiated
      
      if(anyInstantiated){
        Executable.instantiated = true
        ScalarScalarTransformation(instantiatedLeft, instantiatedRight, operation)
      }else
        this
    }
  }

  case class UnaryScalarTransformation(scalar: ScalarRef, operation: UnaryScalarOperation) extends ScalarRef {
    def instantiate(args: Executable*): UnaryScalarTransformation = {
      val instantiatedScalar = scalar.instantiate(args: _*)
      
      if(Executable.instantiated)
        UnaryScalarTransformation(instantiatedScalar, operation)
      else
        this
    }
  }

  sealed trait FunctionRef extends ExpressionExecutable {
    def apply(args: ExpressionExecutable*): Executable

    def instantiate(args: Executable*): FunctionRef
  }
  case class function(numParameters: Int, body: Executable) extends FunctionRef {
    def instantiate(args: Executable*): function = {
      val instantiatedBody = body.instantiate(args: _*)
      
      if(Executable.instantiated)
        function(numParameters, instantiatedBody)
      else
        this
    }

    def apply(args: ExpressionExecutable*): Executable = {
      require(args.length == numParameters)
      body.instantiate(args: _*)
    }
  }

  sealed trait Parameter extends ExpressionExecutable {
    val position: Int
  }

  case class MatrixParameter(position: Int) extends Parameter with Matrix {
    val rows = None
    val cols = None

    def instantiate(args: Executable*): Matrix = {
      Executable.instantiated = true
      args(position) match {
        case x: Matrix => x
        case _ => throw new InstantiationRuntimeError("Argument at position " + position + " is not of type matrix")
      }
    }
  }
  case class StringParameter(position: Int) extends Parameter with StringRef {
    val length = -1
    val value = ""

    override def instantiate(args: Executable*): StringRef = {
      Executable.instantiated = true
      args(position) match {
        case x: StringRef => x
        case _ => throw new InstantiationRuntimeError("Argument at position " + position + " is not of type stringRef")
      }
    }
  }
  case class ScalarParameter(position: Int) extends Parameter with ScalarRef {
    override def instantiate(args: Executable*): ScalarRef = {
      Executable.instantiated = true
      args(position) match {
        case x: ScalarRef => x
        case _ => throw new InstantiationRuntimeError("Argument at position " + position + " is not of type scalarRef")
      }
    }
  }

  case class FunctionParameter(position: Int) extends Parameter with FunctionRef {
    def instantiate(args: Executable*): FunctionRef = {
      Executable.instantiated = true
      args(position) match {
        case x: FunctionRef => x
        case _ => throw new InstantiationRuntimeError("Argument at position " + position + " is not of type functionRef")
      }
    }

    def apply(args: ExpressionExecutable*): Executable = {
      VoidExecutable
    }
  }

}
