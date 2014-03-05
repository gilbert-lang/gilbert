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

  case object VoidExecutable extends ExpressionExecutable {
    def instantiate(args: Executable*) = {
      Executable.instantiated = false
      VoidExecutable
    }

    def getType = Void
  }

  sealed trait Matrix extends ExpressionExecutable {
    def rows: Option[Int]
    def cols: Option[Int]

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

  }

  sealed trait BinaryMatrixTransformation extends Matrix

  sealed trait UnaryMatrixTransformation extends Matrix

  sealed trait FunctionMatrixTransformation extends Matrix

  case class ScalarMatrixTransformation(scalar: ScalarRef, matrix: Matrix, operation: ScalarMatrixOperation)
    extends BinaryMatrixTransformation {
    val rows = matrix.rows
    val cols = matrix.cols

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
        case _: LogicOperation | _: ComparisonOperation => MatrixType(BooleanType)
        case _ => MatrixType(DoubleType)
      }
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

      if (anyInstantiated) {
        Executable.instantiated = true
        MatrixScalarTransformation(instantiatedMatrix, instantiatedScalar, operation)
      } else
        this
    }

    def getType = {
      operation match {
        case _: LogicOperation | _: ComparisonOperation => MatrixType(BooleanType)
        case _ => MatrixType(DoubleType)
      }
    }
  }

  case class MatrixMult(left: Matrix, right: Matrix) extends BinaryMatrixTransformation {
    val rows = left.rows
    val cols = right.cols

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

    def getType = MatrixType(DoubleType)
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

      if (anyInstantiated) {
        Executable.instantiated = true
        CellwiseMatrixMatrixTransformation(instantiatedLeft, instantiatedRight, operation)
      } else
        this
    }

    def getType = {
      operation match {
        case _: LogicOperation | _: ComparisonOperation => MatrixType(BooleanType)
        case _ => MatrixType(DoubleType)
      }
    }
  }

  case class Transpose(matrix: Matrix) extends UnaryMatrixTransformation {
    val rows = matrix.cols
    val cols = matrix.rows

    def instantiate(args: Executable*): Transpose = {
      val instantiatedMatrix = matrix.instantiate(args: _*)

      if (Executable.instantiated)
        Transpose(instantiatedMatrix)
      else
        this
    }

    def getType = matrix.getType
  }

  case class CellwiseMatrixTransformation(matrix: Matrix, operation: UnaryScalarOperation)
    extends UnaryMatrixTransformation {
    val rows = matrix.rows
    val cols = matrix.cols

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

      if (Executable.instantiated)
        VectorwiseMatrixTransformation(instantiatedMatrix, operation)
      else
        this
    }

    def getType = matrix.getType
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

      if (anyInstantiated) {
        Executable.instantiated = true
        LoadMatrix(instantiatedPath, instantiatedRows, instantiatedCols)
      } else {
        this
      }
    }

    def getType = MatrixType(DoubleType)
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

      if (anyInstantiated) {
        Executable.instantiated = true
        ones(instantiatedRows, instantiatedCols)
      } else
        this
    }

    def getType = MatrixType(DoubleType)
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

    def getType = MatrixType(DoubleType)
  }

  case class spones(matrix: Matrix) extends FunctionMatrixTransformation {
    val rows = matrix.rows
    val cols = matrix.cols

    def instantiate(args: Executable*): spones = {
      val instantiatedMatrix = matrix.instantiate(args: _*)

      if (Executable.instantiated)
        spones(instantiatedMatrix)
      else
        this
    }

    def getType = MatrixType(DoubleType)
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

      if (anyInstantiated) {
        Executable.instantiated = true
        sum(instantiatedMatrix, instantiatedDimension)
      } else
        this
    }

    def getType = matrix.getType
  }

  case class sumRow(matrix: Matrix) extends FunctionMatrixTransformation {
    val rows = matrix.rows
    val cols = Some(1)

    def instantiate(args: Executable*): sumRow = {
      val instantiatedMatrix = matrix.instantiate(args: _*)

      if (Executable.instantiated)
        sumRow(instantiatedMatrix)
      else
        this
    }

    def getType = matrix.getType
  }

  case class sumCol(matrix: Matrix) extends FunctionMatrixTransformation {
    val rows = Some(1)
    val cols = matrix.cols

    def instantiate(args: Executable*): sumCol = {
      val instantiatedMatrix = matrix.instantiate(args: _*)

      if (Executable.instantiated)
        sumCol(instantiatedMatrix)
      else
        this
    }

    def getType = matrix.getType
  }

  case class diag(matrix: Matrix) extends FunctionMatrixTransformation {
    val (rows, cols) = getRowsCols

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

    def getType = matrix.getType
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

      if (anyInstantiated) {
        Executable.instantiated = true
        zeros(instantiatedRows, instantiatedCols)
      } else
        this
    }

    def getType = MatrixType(DoubleType)
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

      if (anyInstantiated) {
        Executable.instantiated = true
        eye(instantiatedRows, instantiatedCols)
      } else
        this
    }

    def getType = MatrixType(DoubleType)
  }

  case class FixpointIteration(initialState: Matrix, updateFunction: FunctionRef,
                               maxIterations: ScalarRef, convergence: FunctionRef) extends Matrix {
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
      val instantiatedConvergence = convergence.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

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
      val instantiatedConvergence = convergence.instantiate(args: _*)
      anyInstantiated |= Executable.instantiated

      if (anyInstantiated) {
        Executable.instantiated = true
        FixpointIterationCellArray(instantiatedState, instantiatedUpdateFunction,
          instantiatedMaxIterations, instantiatedConvergence)
      } else
        this
    }

    override def getType = initialState.getType

    def elements = initialState.elements

  }

  case class IterationStatePlaceholderCellArray(cellArrayType: RuntimeTypes.CellArrayType) extends CellArrayBase{
    def elements: List[CellArrayReference[ExpressionExecutable]] = {
      (cellArrayType.elementTypes zipWithIndex) map { case (x, index) => createCellArrayReference(x, index, this) }
    }

    def instantiate(args: Executable*): CellArrayBase = {
      Executable.instantiated = false
      this
    }

    override def getType = cellArrayType
  }

  case object IterationStatePlaceholder extends Matrix {
    val rows = None
    val cols = None

    def instantiate(args: Executable*): Matrix = {
      Executable.instantiated = false
      IterationStatePlaceholder
    }

    def getType = Undefined
  }

  sealed trait StringRef extends ExpressionExecutable {
    def length: Int
    def value: String

    def instantiate(args: Executable*): StringRef

    def getType = StringType
  }

  case class string(value: String) extends StringRef {
    val length = value.length

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
        case _: LogicOperation | _: ComparisonOperation => MatrixType(BooleanType)
        case _ => MatrixType(DoubleType)
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
    val rows = None
    val cols = None

    def instantiate(args: Executable*): Matrix = {
      Executable.instantiated = true
      args(position) match {
        case x: Matrix => x
        case _ => throw new InstantiationRuntimeError("Argument at position " + position + " is not of type matrix")
      }
    }

    def getType = MatrixType(Unknown)
  }

  case class CellArrayReferenceMatrix(parent: CellArrayBase, reference: Int) extends CellArrayReference[Matrix] with
  Matrix{
    def rows = {
      if(extract == this){
        None
      }else{
        extract.rows
      }
    }
    def cols = {
      if(extract == this){
        None
      }else{
        extract.cols
      }
    }

    override def instantiate(args: Executable*): CellArrayReferenceMatrix = {
      val instantiatedParent = parent.instantiate(args:_*)

      if(Executable.instantiated){
        CellArrayReferenceMatrix(instantiatedParent, reference)
      }else{
        this
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

  case class CellArrayReferenceString(parent: CellArrayBase, reference: Int) extends CellArrayReference[StringRef]
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

    def length = {
      if(extract == this){
        -1
      }else{
        extract.length
      }
    }

    def value = {
      if(extract == this){
        ""
      }else{
        extract.value
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

    def getType = Unknown
  }

  case class CellArrayReferenceScalar(parent: CellArrayBase, reference: Int) extends
  CellArrayReference[ScalarRef] with ScalarRef {

    override def instantiate(args: Executable*): CellArrayReferenceScalar = {
      val instantiatedParent = parent.instantiate(args:_*)

      if(Executable.instantiated){
        CellArrayReferenceScalar(instantiatedParent, reference)
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

  case class CellArrayReferenceFunction(parent: CellArrayBase, reference: Int) extends
  CellArrayReference[FunctionRef] with FunctionRef {

    override def apply(args: ExpressionExecutable*): Executable = {
      if(extract == this){
        VoidExecutable
      }else{
        extract.apply(args:_*)
      }
    }

    override def instantiate(args: Executable*): CellArrayReferenceFunction = {
      val instantiatedParent = parent.instantiate(args: _*)

      if(Executable.instantiated){
        CellArrayReferenceFunction(instantiatedParent, reference)
      }else{
        this
      }
    }
  }



  case class RegisteredValue(index: Int) extends Matrix {
    def instantiate(args: Executable*): RegisteredValue = {
      Executable.instantiated = false
      this
    }

    val rows = None
    val cols = None

    def getType = MatrixType(Unknown)
  }

  sealed trait CellArrayBase extends ExpressionExecutable {
    def elements: List[ExpressionExecutable]
    def instantiate(args: Executable*): CellArrayBase

    def getType = CellArrayType(elements map { x => x.getType })

    def rows = Some(1)
    def cols = Some(elements.length)

    protected def createCellArrayReference(tpe: RuntimeType, index: Int,
                                         parent: CellArrayBase): CellArrayReference[ExpressionExecutable] = {
      tpe match {
        case ScalarType => CellArrayReferenceScalar(parent, index)
        case StringType => CellArrayReferenceString(parent, index)
        case _:MatrixType => CellArrayReferenceMatrix(parent, index)
        case FunctionType => CellArrayReferenceFunction(parent, index)
        case x:CellArrayType => CellArrayReferenceCellArray(parent, index, x )
      }
    }
  }

  case class CellArrayExecutable(elements: List[ExpressionExecutable]) extends CellArrayBase{
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

    def elements: List[CellArrayReference[ExpressionExecutable]] = {
      (cellArrayType.elementTypes zipWithIndex) map { case (x, index) => createCellArrayReference(x, index, this) }
    }

    override def getType = cellArrayType
  }

  sealed trait CellArrayReference[+T <: ExpressionExecutable] extends ExpressionExecutable{
    def parent: CellArrayBase
    def reference: Int

    def extract: T = {
      parent.elements(reference).asInstanceOf[T]
    }

    def getType = {
      parent.getType.elementTypes(reference)
    }
  }

  
  case class CellArrayReferenceCellArray(parent: CellArrayBase,
                                         reference: Int, cellArrayType: CellArrayType) extends
  CellArrayReference[CellArrayBase] with
  CellArrayBase {
    def instantiate(args: Executable*):CellArrayReferenceCellArray = {
      val instantiatedParent = parent.instantiate(args: _*)
      
      if(Executable.instantiated){
        CellArrayReferenceCellArray(instantiatedParent, reference, cellArrayType)
      }else{
        this
      }
    }

    override def getType = cellArrayType

    def elements: List[CellArrayReference[ExpressionExecutable]] = {
      (cellArrayType.elementTypes zipWithIndex) map { case (x, index) => createCellArrayReference(x, index, this) }
    }
  }

}
