package org.gilbertlang.runtime

import Operations._

object Executables {

  sealed trait Executable {
    val id: Int = IDGenerator.nextID()

    def instantiate(args: Executable*): Executable
  }

  case class CompoundExecutable(executables: List[Executable]) extends Executable {
    def instantiate(args: Executable*): CompoundExecutable = {
      CompoundExecutable(executables map { _.instantiate(args: _*) })
    }
  }
  case class WriteMatrix(matrix: Matrix) extends Executable {
    def instantiate(args: Executable*): WriteMatrix = {
      WriteMatrix(matrix.instantiate(args: _*))
    }
  }
  case class WriteScalarRef(scalar: ScalarRef) extends Executable {
    def instantiate(args: Executable*): WriteScalarRef = {
      WriteScalarRef(scalar.instantiate(args: _*))
    }
  }

  case class WriteString(string: StringRef) extends Executable {
    def instantiate(args: Executable*): WriteString = {
      WriteString(string.instantiate(args: _*))
    }
  }

  case class WriteFunction(function: FunctionRef) extends Executable {
    def instantiate(args: Executable*): WriteFunction = {
      WriteFunction(function.instantiate(args: _*))
    }
  }

  sealed trait ExpressionExecutable extends Executable

  case object VoidExecutable extends ExpressionExecutable {
    def instantiate(args: Executable*) = VoidExecutable
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
      ScalarMatrixTransformation(scalar.instantiate(args: _*), matrix.instantiate(args: _*), operation)
    }
  }
  case class MatrixScalarTransformation(matrix: Matrix, scalar: ScalarRef, operation: ScalarMatrixOperation)
    extends BinaryMatrixTransformation {
    val rows = matrix.rows
    val cols = matrix.cols

    def instantiate(args: Executable*): MatrixScalarTransformation = {
      MatrixScalarTransformation(matrix.instantiate(args: _*), scalar.instantiate(args: _*), operation)
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
      CellwiseMatrixMatrixTransformation(left.instantiate(args: _*), right.instantiate(args: _*), operation)
    }
  }

  case class Transpose(matrix: Matrix) extends UnaryMatrixTransformation {
    val rows = matrix.cols
    val cols = matrix.rows

    def instantiate(args: Executable*): Transpose = {
      Transpose(matrix.instantiate(args: _*))
    }
  }
  case class CellwiseMatrixTransformation(matrix: Matrix, operation: UnaryScalarOperation)
    extends UnaryMatrixTransformation {
    val rows = matrix.rows
    val cols = matrix.cols

    def instantiate(args: Executable*): CellwiseMatrixTransformation = {
      CellwiseMatrixTransformation(matrix.instantiate(args: _*), operation)
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
      VectorwiseMatrixTransformation(matrix.instantiate(args: _*), operation)
    }
  }

  case class LoadMatrix(path: StringRef, numRows: ScalarRef, numColumns: ScalarRef) extends FunctionMatrixTransformation {
    val rows = scalarRef2Int(numRows)
    val cols = scalarRef2Int(numColumns)

    def instantiate(args: Executable*): LoadMatrix = {
      LoadMatrix(path.instantiate(args: _*), numRows.instantiate(args: _*), numColumns.instantiate(args: _*))
    }
  }
  case class ones(numRows: ScalarRef, numColumns: ScalarRef) extends FunctionMatrixTransformation {
    val rows = scalarRef2Int(numRows)
    val cols = scalarRef2Int(numColumns)

    def instantiate(args: Executable*): ones = {
      ones(numRows.instantiate(args: _*), numColumns.instantiate(args: _*))
    }
  }
  case class randn(numRows: ScalarRef, numColumns: ScalarRef, mean: ScalarRef = scalar(0), std: ScalarRef = scalar(1))
    extends FunctionMatrixTransformation {
    val rows = scalarRef2Int(numRows)
    val cols = scalarRef2Int(numColumns)

    def instantiate(args: Executable*): randn = {
      randn(numRows.instantiate(args: _*), numColumns.instantiate(args: _*), mean.instantiate(args: _*),
        std.instantiate(args: _*))
    }
  }

  case class spones(matrix: Matrix) extends FunctionMatrixTransformation {
    val rows = matrix.rows
    val cols = matrix.cols

    def instantiate(args: Executable*): spones = {
      spones(matrix.instantiate(args: _*))
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
      sum(matrix.instantiate(args: _*), dimension.instantiate(args: _*))
    }
  }

  case class sumRow(matrix: Matrix) extends FunctionMatrixTransformation {
    val rows = matrix.rows
    val cols = Some(1)

    def instantiate(args: Executable*): sumRow = {
      sumRow(matrix.instantiate(args: _*))
    }
  }

  case class sumCol(matrix: Matrix) extends FunctionMatrixTransformation {
    val rows = Some(1)
    val cols = matrix.cols

    def instantiate(args: Executable*): sumCol = {
      sumCol(matrix.instantiate(args: _*))
    }
  }

  case class diag(matrix: Matrix) extends FunctionMatrixTransformation {
    val (rows, cols) = getRowsCols

    def instantiate(args: Executable*): diag = {
      diag(matrix.instantiate(args: _*))
    }

    def getRowsCols: (Option[Int], Option[Int]) = {
      (matrix.rows, matrix.cols) match {
        case (None, _) => (None, None)
        case (_, None) => (None, None)
        case (Some(1), y) => (y, y)
        case (x, Some(1)) => (x, x)
        case (Some(x), Some(y)) => {
          val result = Some(math.min(x, y))
          (result, result)
        }
      }
    }
  }

  case class FixpointIteration(initialState: Matrix, updateFunction: FunctionRef) extends Matrix {
    val updatePlan = updateFunction.apply(IterationStatePlaceholder)

    val rows = initialState.rows
    val cols = initialState.cols

    def instantiate(args: Executable*): FixpointIteration = {
      FixpointIteration(initialState.instantiate(args: _*), updateFunction.instantiate(args: _*))
    }
  }
  case object IterationStatePlaceholder extends Matrix {
    val rows = None
    val cols = None

    def instantiate(args: Executable*): Matrix = IterationStatePlaceholder
  }

  sealed trait StringRef extends ExpressionExecutable {
    val length: Int
    val value: String

    def instantiate(args: Executable*): StringRef = this
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
    def instantiate(args: Executable*): scalar = this
  }

  case class AggregateMatrixTransformation(matrix: Matrix, operation: AggregateMatrixOperation) extends ScalarRef {
    def instantiate(args: Executable*): AggregateMatrixTransformation = {
      AggregateMatrixTransformation(matrix.instantiate(args: _*), operation)
    }
  }

  case class ScalarScalarTransformation(left: ScalarRef, right: ScalarRef, operation: ScalarsOperation) extends ScalarRef {
    def instantiate(args: Executable*): ScalarScalarTransformation = {
      ScalarScalarTransformation(left.instantiate(args: _*), right.instantiate(args: _*), operation)
    }
  }

  case class UnaryScalarTransformation(scalar: ScalarRef, operation: UnaryScalarOperation) extends ScalarRef {
    def instantiate(args: Executable*): UnaryScalarTransformation = {
      UnaryScalarTransformation(scalar.instantiate(args: _*), operation)
    }
  }

  sealed trait FunctionRef extends ExpressionExecutable {
    def apply(args: ExpressionExecutable*): Executable

    def instantiate(args: Executable*): FunctionRef
  }
  case class function(numParameters: Int, body: Executable) extends FunctionRef {
    def instantiate(args: Executable*): function = {
      function(numParameters, body.instantiate(args: _*))
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
      args(position) match {
        case x: StringRef => x
        case _ => throw new InstantiationRuntimeError("Argument at position " + position + " is not of type stringRef")
      }
    }
  }
  case class ScalarParameter(position: Int) extends Parameter with ScalarRef {
    override def instantiate(args: Executable*): ScalarRef = {
      args(position) match {
        case x: ScalarRef => x
        case _ => throw new InstantiationRuntimeError("Argument at position " + position + " is not of type scalarRef")
      }
    }
  }

  case class FunctionParameter(position: Int) extends Parameter with FunctionRef {
    def instantiate(args: Executable*): FunctionRef = {
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
