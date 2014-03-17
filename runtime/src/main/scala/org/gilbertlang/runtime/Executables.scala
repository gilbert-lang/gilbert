package org.gilbertlang.runtime

import Operations._
import org.gilbertlang.runtime.RuntimeTypes._

object Executables {

  sealed trait Executable {
    val id: Int = IDGenerator.nextID()

    def instantiate(args: Executable*): Executable

    def getType: RuntimeType
  }

  object Executable {
    var instantiated: Boolean = false
  }

  case class CompoundExecutable(executables: List[Executable]) extends Executable {
    def instantiate(args: Executable*): CompoundExecutable = {
      var anyInstantiated = false
      val instantiatedExecutables = executables map {
        exec =>
          val instantiatedExec = exec.instantiate(args: _*)
          anyInstantiated |= Executable.instantiated
          instantiatedExec
      }

      if (anyInstantiated) {
        Executable.instantiated = true
        CompoundExecutable(instantiatedExecutables)
      } else
        this
    }

    def getType = Void
  }

  case class WriteMatrix(matrix: Matrix) extends Executable {
    def instantiate(args: Executable*): WriteMatrix = {
      val instantiatedMatrix = matrix.instantiate(args: _*)

      if (Executable.instantiated){
        Executable.instantiated = true
        WriteMatrix(instantiatedMatrix)
      }
      else
        this
    }

    def getType = Void
  }

  case class WriteScalar(scalar: ScalarRef) extends Executable {
    def instantiate(args: Executable*): WriteScalar = {
      val instantiatedScalar = scalar.instantiate(args: _*)

      if (Executable.instantiated){
        Executable.instantiated = true
        WriteScalar(instantiatedScalar)
      }
      else
        this
    }

    def getType = Void
  }

  case class WriteString(string: StringRef) extends Executable {
    def instantiate(args: Executable*): WriteString = {
      val instantiatedString = string.instantiate(args: _*)

      if (Executable.instantiated){
        Executable.instantiated = true
        WriteString(instantiatedString)
      }
      else
        this
    }

    def getType = Void
  }

  case class WriteCellArray(cellArray: CellArrayBase) extends Executable {
    def instantiate(args: Executable*): WriteCellArray = {
      val instantiatedCellArray = cellArray.instantiate(args: _*)

      if(Executable.instantiated){
        WriteCellArray(instantiatedCellArray)
      }else{
        this
      }
    }

    def getType = Void

  }

  case class WriteFunction(function: FunctionRef) extends Executable {
    def instantiate(args: Executable*): WriteFunction = {
      val instantiatedFunction = function.instantiate(args: _*)

      if (Executable.instantiated){
        Executable.instantiated = true
        WriteFunction(instantiatedFunction)
      }
      else
        this
    }

    def getType = Void
  }

  sealed trait ExpressionExecutable extends Executable {
    def instantiate(args: Executable*): ExpressionExecutable
  }

  case object VoidExecutable extends FunctionRef {
    def instantiate(args: Executable*) = {
      Executable.instantiated = false
      VoidExecutable
    }

    def apply(args: ExpressionExecutable*) = VoidExecutable

    def getType = Void
  }

  sealed trait Matrix extends ExpressionExecutable {
    def rows = getType.rows
    def cols = getType.cols
    def elementType = getType.elementType

    def transpose() = {
      Transpose(this)
    }

    def times(other: Matrix) = {
      MatrixMult(this, other)
    }

    def times(scalar: ScalarRef) = {
      ScalarMatrixTransformation(scalar, this, Multiplication)
    }

    def div(scalar: ScalarRef) = {
      ScalarMatrixTransformation(scalar, this, Division)
    }

    def plus(other: Matrix) = {
      CellwiseMatrixMatrixTransformation(this, other, Addition)
    }

    def binarize() = {
      CellwiseMatrixTransformation(this, Binarize)
    }

    def max() = {
      AggregateMatrixTransformation(this, Maximum)
    }

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

    def <= (scalar: ScalarRef) = {
      MatrixScalarTransformation(this, scalar, LessEqualThan)
    }

    def normalizeRows(norm: Int) = {
      norm match {
        case 1 => VectorwiseMatrixTransformation(this, NormalizeL1)
      }
    }

    def instantiate(args: Executable*): Matrix

    def getType: MatrixType

  }

  sealed trait BinaryMatrixTransformation extends Matrix

  sealed trait UnaryMatrixTransformation extends Matrix

  sealed trait FunctionMatrixTransformation extends Matrix

  case class ScalarMatrixTransformation(scalar: ScalarRef, matrix: Matrix, operation: ScalarMatrixOperation)
    extends BinaryMatrixTransformation {

    def instantiate(args: Executable*): ScalarMatrixTransformation = {
      var anyInstantiated = false
      val instantiatedScalar = scalar.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedMatrix = matrix.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

      if (anyInstantiated) {
        Executable.instantiated = true
        ScalarMatrixTransformation(instantiatedScalar, instantiatedMatrix, operation)
      } else
        this
    }

    def getType = {
      operation match {
        case _: LogicOperation | _: ComparisonOperation => MatrixType(BooleanType, matrix.rows, matrix.cols)
        case _ => MatrixType(DoubleType, matrix.rows, matrix. cols)
      }
    }
  }

  case class MatrixScalarTransformation(matrix: Matrix, scalar: ScalarRef, operation: ScalarMatrixOperation)
    extends BinaryMatrixTransformation {

    def instantiate(args: Executable*): MatrixScalarTransformation = {
      var anyInstantiated = false
      val instantiatedMatrix = matrix.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedScalar = scalar.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

      if (anyInstantiated) {
        Executable.instantiated = true
        MatrixScalarTransformation(instantiatedMatrix, instantiatedScalar, operation)
      } else
        this
    }

    def getType = {
      operation match {
        case _: LogicOperation | _: ComparisonOperation => MatrixType(BooleanType, matrix.rows, matrix.cols)
        case _ => MatrixType(DoubleType, matrix.rows, matrix.cols)
      }
    }
  }

  case class MatrixMult(left: Matrix, right: Matrix) extends BinaryMatrixTransformation {

    def instantiate(args: Executable*): MatrixMult = {
      var anyInstantiated = false
      val instantiatedLeft = left.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedRight = right.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

      if(anyInstantiated){
        Executable.instantiated = true
        MatrixMult(instantiatedLeft, instantiatedRight)
      }else
        this
    }

    def getType = MatrixType(DoubleType, left.rows, right.cols)
  }

  case class CellwiseMatrixMatrixTransformation(left: Matrix, right: Matrix, operation: CellwiseOperation)
    extends BinaryMatrixTransformation {

    def instantiate(args: Executable*): CellwiseMatrixMatrixTransformation = {
      var anyInstantiated = false
      val instantiatedLeft = left.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedRight = right.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

      if (anyInstantiated) {
        Executable.instantiated = true
        CellwiseMatrixMatrixTransformation(instantiatedLeft, instantiatedRight, operation)
      } else
        this
    }

    def getType = {
      operation match {
        case _: LogicOperation | _: ComparisonOperation => MatrixType(BooleanType, left.rows, left.cols)
        case _ => MatrixType(DoubleType, left.rows, left.cols)
      }
    }
  }

  case class Transpose(matrix: Matrix) extends UnaryMatrixTransformation {

    def instantiate(args: Executable*): Transpose = {
      val instantiatedMatrix = matrix.instantiate(args: _*)

      if (Executable.instantiated)
        Transpose(instantiatedMatrix)
      else
        this
    }

    def getType = {
      MatrixType(matrix.elementType, matrix.cols, matrix.rows)
    }
  }

  case class CellwiseMatrixTransformation(matrix: Matrix, operation: UnaryScalarOperation)
    extends UnaryMatrixTransformation {

    def instantiate(args: Executable*): CellwiseMatrixTransformation = {
      val instantiatedMatrix = matrix.instantiate(args: _*)

      if (Executable.instantiated)
        CellwiseMatrixTransformation(instantiatedMatrix, operation)
      else
        this
    }

    def getType = matrix.getType
  }

  case class VectorwiseMatrixTransformation(matrix: Matrix, operation: VectorwiseOperation)
    extends UnaryMatrixTransformation {

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

      if (Executable.instantiated)
        VectorwiseMatrixTransformation(instantiatedMatrix, operation)
      else
        this
    }

    def getType = {
      val (rows, cols) = getMatrixSize
      MatrixType(matrix.elementType, rows, cols)
    }
  }

  case class LoadMatrix(path: StringRef, numRows: ScalarRef, numColumns: ScalarRef) extends FunctionMatrixTransformation {

    def instantiate(args: Executable*): LoadMatrix = {
      var anyInstantiated = false
      val instantiatedPath = path.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedRows = numRows.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedCols = numColumns.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

      if (anyInstantiated) {
        Executable.instantiated = true
        LoadMatrix(instantiatedPath, instantiatedRows, instantiatedCols)
      } else {
        this
      }
    }

    def getType = MatrixType(DoubleType, scalarRef2Int(numRows), scalarRef2Int(numColumns))
  }


  case class ones(numRows: ScalarRef, numColumns: ScalarRef) extends FunctionMatrixTransformation {

    def instantiate(args: Executable*): ones = {
      var anyInstantiated = false
      val instantiatedRows = numRows.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedCols = numColumns.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

      if (anyInstantiated) {
        Executable.instantiated = true
        ones(instantiatedRows, instantiatedCols)
      } else
        this
    }

    def getType = MatrixType(DoubleType, scalarRef2Int(numRows), scalarRef2Int(numColumns))
  }

  case class randn(numRows: ScalarRef, numColumns: ScalarRef, mean: ScalarRef = scalar(0), std: ScalarRef = scalar(1))
    extends FunctionMatrixTransformation {

    def instantiate(args: Executable*): randn = {
      var anyInstantiated = false
      val instantiatedRows = numRows.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedCols = numColumns.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedMean = mean.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedStd = std.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

      if (anyInstantiated) {
        Executable.instantiated = true
        randn(instantiatedRows, instantiatedCols, instantiatedMean, instantiatedStd)
      } else
        this
    }

    def getType = MatrixType(DoubleType, scalarRef2Int(numRows), scalarRef2Int(numColumns))
  }

  case class spones(matrix: Matrix) extends FunctionMatrixTransformation {

    def instantiate(args: Executable*): spones = {
      val instantiatedMatrix = matrix.instantiate(args: _*)

      if (Executable.instantiated)
        spones(instantiatedMatrix)
      else
        this
    }

    def getType = matrix.getType
  }

  //TODO remove
  case class sum(matrix: Matrix, dimension: ScalarRef) extends FunctionMatrixTransformation {

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

      if (anyInstantiated) {
        Executable.instantiated = true
        sum(instantiatedMatrix, instantiatedDimension)
      } else
        this
    }

    def getType = {
      MatrixType(matrix.elementType, getRows, getCols)
    }
  }

  case class sumRow(matrix: Matrix) extends FunctionMatrixTransformation {

    def instantiate(args: Executable*): sumRow = {
      val instantiatedMatrix = matrix.instantiate(args: _*)

      if (Executable.instantiated)
        sumRow(instantiatedMatrix)
      else
        this
    }

    def getType = {
      MatrixType(matrix.elementType, matrix.rows, Some(1))
    }
  }

  case class sumCol(matrix: Matrix) extends FunctionMatrixTransformation {

    def instantiate(args: Executable*): sumCol = {
      val instantiatedMatrix = matrix.instantiate(args: _*)

      if (Executable.instantiated)
        sumCol(instantiatedMatrix)
      else
        this
    }

    def getType = MatrixType(matrix.elementType, Some(1), matrix.cols)
  }

  case class diag(matrix: Matrix) extends FunctionMatrixTransformation {

    def instantiate(args: Executable*): diag = {
      val instantiatedMatrix = matrix.instantiate(args: _*)

      if (Executable.instantiated)
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

    def getType = {
      val (rows, cols) = getRowsCols
      MatrixType(matrix.elementType, rows, cols)
    }
  }

  case class zeros(numRows: ScalarRef, numCols: ScalarRef) extends FunctionMatrixTransformation {

    def instantiate(args: Executable*): zeros = {
      var anyInstantiated = false
      val instantiatedRows = numRows.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedCols = numCols.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

      if (anyInstantiated) {
        Executable.instantiated = true
        zeros(instantiatedRows, instantiatedCols)
      } else
        this
    }

    def getType = MatrixType(DoubleType, scalarRef2Int(numRows), scalarRef2Int(numCols))
  }

  case class eye(numRows: ScalarRef, numCols: ScalarRef) extends FunctionMatrixTransformation {

    def instantiate(args: Executable*): eye = {
      var anyInstantiated = false
      val instantiatedRows = numRows.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedCols = numCols.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

      if (anyInstantiated) {
        Executable.instantiated = true
        eye(instantiatedRows, instantiatedCols)
      } else
        this
    }

    def getType = MatrixType(DoubleType, scalarRef2Int(numRows), scalarRef2Int(numCols))
  }

  case class FixpointIteration(initialState: Matrix, updateFunction: FunctionRef,
                               maxIterations: ScalarRef, convergence: FunctionRef) extends Matrix {
    val updatePlan = updateFunction.apply(IterationStatePlaceholder)


    def instantiate(args: Executable*): FixpointIteration = {
      var anyInstantiated = false
      val instantiatedState = initialState.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedUpdateFunction = updateFunction.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedMaxIterations = maxIterations.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

      var instantiatedConvergence:FunctionRef = null
      if(convergence != null){
        instantiatedConvergence = convergence.instantiate(args: _*)
        anyInstantiated |= Executable.instantiated
      }


      if (anyInstantiated) {
        Executable.instantiated = true
        FixpointIteration(instantiatedState, instantiatedUpdateFunction,
          instantiatedMaxIterations, instantiatedConvergence)
      } else
        this
    }

    def getType = initialState.getType

  }

  case class FixpointIterationCellArray(initialState: CellArrayBase, updateFunction: FunctionRef,
                               maxIterations: ScalarRef, convergence: FunctionRef) extends CellArrayBase {
    val updatePlan = updateFunction.apply(IterationStatePlaceholderCellArray(initialState.getType))

    def instantiate(args: Executable*): FixpointIterationCellArray = {
      var anyInstantiated = false
      val instantiatedState = initialState.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedUpdateFunction = updateFunction.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedMaxIterations = maxIterations.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

      var instantiatedConvergence: FunctionRef = null
      if(convergence != null){
        instantiatedConvergence = convergence.instantiate(args: _*)
        anyInstantiated |= Executable.instantiated
      }

      if (anyInstantiated) {
        Executable.instantiated = true
        FixpointIterationCellArray(instantiatedState, instantiatedUpdateFunction,
          instantiatedMaxIterations, instantiatedConvergence)
      } else
        this
    }

    override def getType = initialState.getType
  }

  case class IterationStatePlaceholderCellArray(getType: CellArrayType) extends CellArrayBase{

    def instantiate(args: Executable*): CellArrayBase = {
      Executable.instantiated = false
      this
    }
  }

  case object IterationStatePlaceholder extends Matrix {

    def instantiate(args: Executable*): Matrix = {
      Executable.instantiated = false
      IterationStatePlaceholder
    }

    def getType = MatrixType(Undefined, None, None)
  }

  sealed trait StringRef extends ExpressionExecutable {
    def length: Option[Int]
    def value: String

    def instantiate(args: Executable*): StringRef

    def getType = StringType
  }

  case class string(value: String) extends StringRef {
    val length = Some(value.length)

    override def instantiate(args: Executable*): string = {
      Executable.instantiated = false
      this
    }
  }

  sealed trait ScalarRef extends ExpressionExecutable {

    def *(matrix: Matrix) = {
      ScalarMatrixTransformation(this, matrix, Multiplication)
    }

    def /(matrix: Matrix) = {
      ScalarMatrixTransformation(this, matrix, Division)
    }

    def /(other: ScalarRef) = {
      ScalarScalarTransformation(this, other, Division)
    }

    def -(other: ScalarRef) = {
      ScalarScalarTransformation(this, other, Subtraction)
    }

    def <=(other: ScalarRef) = {
      ScalarScalarTransformation(this, other, LessEqualThan)
    }

    def instantiate(args: Executable*): ScalarRef

    def getType: ScalarType
  }

  sealed trait FunctionScalarTransformation extends ScalarRef

  case class scalar(value: Double) extends ScalarRef {
    def instantiate(args: Executable*): scalar = {
      Executable.instantiated = false
      this
    }

    def getType = DoubleType
  }

  case class boolean(value: Boolean) extends ScalarRef {
    def instantiate(args: Executable*): boolean = {
      Executable.instantiated = false
      this
    }

    def getType = BooleanType
  }

  case class AggregateMatrixTransformation(matrix: Matrix, operation: AggregateMatrixOperation) extends ScalarRef {
    def instantiate(args: Executable*): AggregateMatrixTransformation = {
      val instantiatedMatrix = matrix.instantiate(args: _*)

      if (Executable.instantiated)
        AggregateMatrixTransformation(instantiatedMatrix, operation)
      else
        this
    }

    def getType = DoubleType
  }

  case class ScalarScalarTransformation(left: ScalarRef, right: ScalarRef, operation: ScalarsOperation) extends ScalarRef {
    def instantiate(args: Executable*): ScalarScalarTransformation = {
      var anyInstantiated = false
      val instantiatedLeft = left.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedRight = right.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

      if (anyInstantiated) {
        Executable.instantiated = true
        ScalarScalarTransformation(instantiatedLeft, instantiatedRight, operation)
      } else
        this
    }

    def getType = {
      operation match {
        case _: LogicOperation | _: ComparisonOperation => BooleanType
        case _ => DoubleType
      }
    }
  }

  case class UnaryScalarTransformation(scalar: ScalarRef, operation: UnaryScalarOperation) extends ScalarRef {
    def instantiate(args: Executable*): UnaryScalarTransformation = {
      val instantiatedScalar = scalar.instantiate(args: _*)

      if (Executable.instantiated)
        UnaryScalarTransformation(instantiatedScalar, operation)
      else
        this
    }

    def getType = DoubleType
  }


  case class norm(matrix: Matrix, p: ScalarRef) extends FunctionScalarTransformation {
    override def instantiate(args: Executable*): norm = {
      var anyInstantiated = false
      val instantiatedMatrix = matrix.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedP = p.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

      if (anyInstantiated){
        Executable.instantiated =true
        norm(instantiatedMatrix, instantiatedP)
      }
      else
        this
    }

    def getType = DoubleType
  }

  sealed trait FunctionRef extends ExpressionExecutable {
    def apply(args: ExpressionExecutable*): Executable

    def instantiate(args: Executable*): FunctionRef

    def getType: FunctionType
  }

  case class function(numParameters: Int, body: Executable) extends FunctionRef {
    def instantiate(args: Executable*): function = {
      val instantiatedBody = body.instantiate(args: _*)

      if (Executable.instantiated)
        function(numParameters, instantiatedBody)
      else
        this
    }

    def apply(args: ExpressionExecutable*): Executable = {
      require(args.length == numParameters)
      body.instantiate(args: _*)
    }

    def getType = FunctionType
  }

  sealed trait Parameter extends ExpressionExecutable {
    val position: Int
  }

  case class MatrixParameter(position: Int) extends Parameter with Matrix {

    def instantiate(args: Executable*): Matrix = {
      Executable.instantiated = true
      args(position) match {
        case x: Matrix => x
        case _ => throw new InstantiationRuntimeError("Argument at position " + position + " is not of type matrix")
      }
    }

    def getType = MatrixType(Unknown, None, None)
  }

  case class CellArrayReferenceMatrix(parent: CellArrayBase, reference: Int,
                                      getType: MatrixType) extends CellArrayReference with
  Matrix{
    override def instantiate(args: Executable*): CellArrayReferenceMatrix = {
      val instantiatedParent = parent.instantiate(args:_*)

      if(Executable.instantiated){
        CellArrayReferenceMatrix(instantiatedParent, reference, getType)
      }else{
        this
      }
    }
  }

  case class StringParameter(position: Int) extends Parameter with StringRef {
    val length = None
    val value = ""

    override def instantiate(args: Executable*): StringRef = {
      Executable.instantiated = true
      args(position) match {
        case x: StringRef => x
        case _ => throw new InstantiationRuntimeError("Argument at position " + position + " is not of type stringRef")
      }
    }
  }

  case class CellArrayReferenceString(parent: CellArrayBase, reference: Int) extends CellArrayReference
  with StringRef {
    override def getType = StringType

    override def instantiate(args: Executable*): CellArrayReferenceString = {
      val instantiatedParent = parent.instantiate(args:_*)

      if(Executable.instantiated){
        CellArrayReferenceString(instantiatedParent, reference)
      }else{
        this
      }
    }

    def length = None

    def value = ""

  }

  case class ScalarParameter(position: Int) extends Parameter with ScalarRef {
    override def instantiate(args: Executable*): ScalarRef = {
      Executable.instantiated = true
      args(position) match {
        case x: ScalarRef => x
        case _ => throw new InstantiationRuntimeError("Argument at position " + position + " is not of type scalarRef")
      }
    }

    def getType = ScalarType
  }

  case class CellArrayReferenceScalar(parent: CellArrayBase, reference: Int, getType: ScalarType) extends
  CellArrayReference with ScalarRef {

    override def instantiate(args: Executable*): CellArrayReferenceScalar = {
      val instantiatedParent = parent.instantiate(args:_*)

      if(Executable.instantiated){
        CellArrayReferenceScalar(instantiatedParent, reference, getType)
      }else{
        this
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

    def getType = Unknown
  }

  case object ConvergencePreviousStatePlaceholder extends Matrix {
    override def instantiate(args: Executable*) = {
      Executable.instantiated = false
      this
    }

    def getType = MatrixType(Unknown, None, None)
  }

  case object ConvergenceCurrentStatePlaceholder extends Matrix {
    override def instantiate(args: Executable*) = {
      Executable.instantiated = false
      this
    }


    def getType = MatrixType(Unknown, None, None)
  }

  sealed trait CellArrayBase extends ExpressionExecutable {
    def instantiate(args: Executable*): CellArrayBase

    def getType:CellArrayType

    def rows = Some(1)
    def cols = Some(getType.elementTypes.length)

    def apply(index: Int): ExpressionExecutable = {
      createCellArrayReference(index)
    }

    protected def createCellArrayReference(index: Int): CellArrayReference = {
      getType.elementTypes(index) match {
        case x:ScalarType => CellArrayReferenceScalar(this, index,x)
        case StringType => CellArrayReferenceString(this, index)
        case x:MatrixType => CellArrayReferenceMatrix(this, index, x)
        case x:FunctionType => throw new IllegalArgumentException("Function type cell entries are not supported")
        case x:CellArrayType => CellArrayReferenceCellArray(this, index, x )
      }
    }
  }

  sealed trait CellArrayAccessibleBase extends CellArrayBase {
    def elements: List[ExpressionExecutable]
    def instantiate(args: Executable*): CellArrayAccessibleBase

    override def getType = CellArrayType(elements map { _.getType })
    override def cols = Some(elements.length)

    override def apply(index: Int): ExpressionExecutable = {
      elements(index)
    }
  }

  case class CellArrayExecutable(elements: List[ExpressionExecutable]) extends CellArrayAccessibleBase{
    def instantiate(args: Executable*): CellArrayExecutable = {
      var anyInstantiated = false
      val instantiatedElements = elements map { element =>
        val result = element.instantiate(args: _*)
        anyInstantiated |= Executable.instantiated
        result
      }

      if(anyInstantiated){
        Executable.instantiated = true
        CellArrayExecutable(instantiatedElements)
      }else
        this
    }
  }

  case class CellArrayParameter(position: Int, cellArrayType: CellArrayType) extends CellArrayBase with
  Parameter{
    override def instantiate(args: Executable*):CellArrayBase = {
      Executable.instantiated = true
      args(position) match {
        case x: CellArrayBase => x
        case _ => throw new InstantiationRuntimeError(s"Argument at position $position is not of type " +
          s"CellArrayExecutable")
      }

    }

    override def getType = cellArrayType
  }

  sealed trait CellArrayReference extends ExpressionExecutable{
    def parent: CellArrayBase
    def reference: Int
  }

  
  case class CellArrayReferenceCellArray(parent: CellArrayBase, reference: Int, getType: CellArrayType) extends
  CellArrayReference with CellArrayBase {
    def instantiate(args: Executable*):CellArrayReferenceCellArray = {
      val instantiatedParent = parent.instantiate(args: _*)
      
      if(Executable.instantiated){
        CellArrayReferenceCellArray(instantiatedParent, reference, getType)
      }else{
        this
      }
    }
  }

  case class ConvergenceCurrentStateCellArrayPlaceholder(getType: CellArrayType) extends
  CellArrayBase{
    def instantiate(args: Executable*): CellArrayBase = {
      Executable.instantiated = false
      this
    }
  }

  case class ConvergencePreviousStateCellArrayPlaceholder(getType: CellArrayType) extends
  CellArrayBase{
    def instantiate(args: Executable*): CellArrayBase = {
      Executable.instantiated = false
      this
    }
  }

  case class TypeConversionScalar(scalar: ScalarRef, sourceType: RuntimeTypes.ScalarType,
                                  targetType: RuntimeTypes.ScalarType) extends
  ScalarRef{
    def getType = targetType

    def instantiate(args: Executable*): TypeConversionScalar = {
      val instantiatedScalar = scalar.instantiate(args:_*)

      if(Executable.instantiated){
        TypeConversionScalar(instantiatedScalar, sourceType, targetType)
      }else{
        this
      }
    }
  }

  case class TypeConversionMatrix(matrix: Matrix, sourceType: RuntimeTypes.MatrixType,
                                  targetType: RuntimeTypes.MatrixType) extends
  Matrix{

    def instantiate(args: Executable*): TypeConversionMatrix = {
      val instantiatedMatrix = matrix.instantiate(args:_*)

      if(Executable.instantiated){
        TypeConversionMatrix(instantiatedMatrix, sourceType, targetType)
      }else{
        this
      }
    }

    def getType = targetType
  }

  case class linspace(start: ScalarRef, end: ScalarRef, numPoints: ScalarRef) extends Matrix {

    def instantiate(args: Executable*): linspace = {
      var anyInstantiated = false
      val instantiatedStart = start.instantiate(args:_*)
      anyInstantiated |= Executable.instantiated
      val instantiatedEnd = end.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated
      val instantiatedNumPoints = numPoints.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

      if(anyInstantiated){
        Executable.instantiated = true
        linspace(instantiatedStart, instantiatedEnd, instantiatedNumPoints)
      }else{
        this
      }
    }

    def getType = MatrixType(DoubleType, Some(1), scalarRef2Int(numPoints))
  }

  case class repmat(matrix: Matrix, numRows: ScalarRef, numCols: ScalarRef) extends Matrix {
    def getRows = {
      (scalarRef2Int(numRows), matrix.rows) match {
        case (Some(a), Some(b)) => Some(a*b)
        case _ => None
      }
    }

    def getCols = {
      (scalarRef2Int(numCols), matrix.cols) match {
        case (Some(a), Some(b)) => Some(a*b)
        case _ => None
      }
    }

    def instantiate(args: Executable*): repmat = {
      var anyInstantiated = false
      val instantiatedMatrix = matrix.instantiate(args:_*)
      anyInstantiated |= Executable.instantiated
      val instantiatedNumRows = numRows.instantiate(args:_*)
      anyInstantiated |= Executable.instantiated
      val instantiatedNumCols = numCols.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

      if(anyInstantiated){
        Executable.instantiated = true
        repmat(instantiatedMatrix, instantiatedNumRows, instantiatedNumCols)
      }else{
        this
      }
    }

    def getType = MatrixType(matrix.elementType, getRows, getCols)
  }

  case class pdist2(matrixA: Matrix, matrixB: Matrix) extends Matrix {

    def instantiate(args: Executable*): pdist2 = {
      var anyInstantiated = false
      val instantiatedMatrixA = matrixA.instantiate(args:_*)
      anyInstantiated |= Executable.instantiated
      val instantiatedMatrixB = matrixB.instantiate(args:_*)
      anyInstantiated |= Executable.instantiated

      if(anyInstantiated){
        Executable.instantiated = true
        pdist2(instantiatedMatrixA, instantiatedMatrixB)
      }else{
        this
      }
    }

    def getType = MatrixType(DoubleType, matrixA.rows, matrixB.rows)
  }

  case class minWithIndex(matrix: Matrix, dimension: ScalarRef) extends CellArrayBase{
    def instantiate(args: Executable*): minWithIndex = {
      var anyInstantiated = false
      val instantiatedMatrix = matrix.instantiate(args:_*)
      anyInstantiated |= Executable.instantiated
      val instantiatedDimension = dimension.instantiate(args:_*)
      anyInstantiated |= Executable.instantiated

      if(anyInstantiated){
        Executable.instantiated = true
        minWithIndex(instantiatedMatrix, instantiatedDimension)
      }else{
        this
      }
    }

    def getRowsCols = {
      scalarRef2Int(dimension) match {
        case None => (None, None)
        case Some(1) => (Some(1), matrix.cols)
        case Some(2) => (matrix.rows, Some(1))
      }
    }

    override def getType = {
      val (rows, cols) = getRowsCols
      CellArrayType(List(MatrixType(DoubleType, rows, cols),
        MatrixType(DoubleType, rows, cols)))
    }
  }

}
