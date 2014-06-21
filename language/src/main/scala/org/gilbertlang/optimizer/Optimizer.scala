package org.gilbertlang.optimizer

import org.gilbertlang.runtime.Executables._

import scala.collection.mutable


object Optimizer {
  val tpMap = new mutable.HashMap[Executable, Executable]()
  val mmMap = new mutable.HashMap[Executable, Executable]()
  val mmOpMap = new mutable.HashMap[(Matrix, Matrix), MatrixMult]()

  /**
   * Pushdown all transpose in order to eliminate them
   * @param program
   * @return
   */
  def transposePushdown(program: Executable): Executable= {
    tpMap.clear()

    def tpExec(program: Executable): Executable = {
      program match {
        case x: Placeholder => x
        case x: Parameter => x
        case x@WriteMatrix(m) => tpMap.getOrElseUpdate(x, WriteMatrix(tpM(m)))
        case x@WriteScalar(s) => tpMap.getOrElseUpdate(x,WriteScalar(tpS(s)))
        case x: WriteString => x
        case x@WriteFunction(f) => tpMap.getOrElseUpdate(x,WriteFunction(tpF(f)))
        case x@WriteCellArray(c) => tpMap.getOrElseUpdate(x,WriteCellArray(tpC(c)))
        case x@CompoundExecutable(exs) => tpMap.getOrElseUpdate(x,CompoundExecutable(exs map (tpExec _)))
        case x: ExpressionExecutable => tpExp(x)
      }
    }

    def tpExp(exp: ExpressionExecutable): ExpressionExecutable = {
      tpMap.getOrElseUpdate(exp,
      exp match {
        case x: Matrix => tpM(x)
        case x: ScalarRef => tpS(x)
        case x: CellArrayBase => tpC(x)
        case x: FunctionRef => tpF(x)
        case x: StringRef => x
      }).asInstanceOf[ExpressionExecutable]
    }

    def tpM(matrix: Matrix): Matrix = {
      tpMap.getOrElseUpdate(matrix,
        matrix match {
        case ScalarMatrixTransformation(s, m, op) => ScalarMatrixTransformation(tpS(s),
          tpM(m), op)
        case MatrixScalarTransformation(m,s,op) => ScalarMatrixTransformation(tpS(s),tpM(m),  op)
        case MatrixMult(a,b) => MatrixMult(tpM(a), tpM(b))
        case CellArrayReferenceMatrix(parent, idx, tpe) => CellArrayReferenceMatrix(tpC(parent), idx, tpe)
        case CellwiseMatrixMatrixTransformation(a,b,op) => CellwiseMatrixMatrixTransformation(tpM(a), tpM(b), op)
        case CellwiseMatrixTransformation(a,op) => CellwiseMatrixTransformation(tpM(a), op)
        case x: Placeholder => x
        case x:LoadMatrix => x
        case x:MatrixParameter => x
        case FixpointIterationMatrix(initial, update, maxIterations, convergence) => FixpointIterationMatrix(tpM
          (initial), tpF(update), tpS(maxIterations), tpF(convergence))
        case TypeConversionMatrix(m, srcType, targetType) => TypeConversionMatrix(tpM(m), srcType, targetType)
        case VectorwiseMatrixTransformation(m, op) => VectorwiseMatrixTransformation(tpM(m), op)
        case diag(m) => diag(tpM(m))
        case eye(rows, cols) => eye(tpS(rows), tpS(cols))
        case linspace(start, end, num) => linspace(tpS(start), tpS(end), tpS(num))
        case ones(rows, cols) => ones(tpS(rows), tpS(cols))
        case randn(rows, cols, mean, std) => randn(tpS(rows), tpS(cols), tpS(mean), tpS(std))
        case pdist2(a,b) => pdist2(tpM(a), tpM(b))
        case repmat(m, a,b) => repmat(tpM(m), tpS(a), tpS(b))
        case spones(m) => spones(tpM(m))
        case sum(m,dim) => sum(tpM(m), tpS(dim))
        case sumCol(m) => sumCol(tpM(m))
        case sumRow(m) => sumRow(tpM(m))
        case zeros(rows, cols) => zeros(tpS(rows), tpS(cols))
        case Transpose(Transpose(m)) => tpM(m)
        case Transpose(ScalarMatrixTransformation(s,m,op)) => ScalarMatrixTransformation(tpS(s), tpM(Transpose(m)), op)
        case Transpose(MatrixScalarTransformation(m,s,op)) => MatrixScalarTransformation(tpM(Transpose(m)), tpS(s), op)
        case Transpose(MatrixMult(a,b)) => MatrixMult(tpM(Transpose(b)), tpM(Transpose(a)))
        case Transpose(CellwiseMatrixMatrixTransformation(a,b,op)) => CellwiseMatrixMatrixTransformation(tpM
          (Transpose(a)), tpM(Transpose(b)), op)
        case Transpose(CellwiseMatrixTransformation(a,op)) => CellwiseMatrixTransformation(tpM(Transpose(a)), op)
        case Transpose(m) => Transpose(tpM(m))
      }).asInstanceOf[Matrix]
    }

    def tpS(s: ScalarRef): ScalarRef = {
      tpMap.getOrElseUpdate(s,
      s match {
        case x: scalar => x
        case b: boolean => b
        case AggregateMatrixTransformation(m, op) => AggregateMatrixTransformation(tpM(m),
          op)
        case ScalarScalarTransformation(a,b,op)=> ScalarScalarTransformation(tpS(a), tpS(b), op)
        case TypeConversionScalar(s, srcType, targetType) => TypeConversionScalar(tpS(s), srcType, targetType)
        case UnaryScalarTransformation(s, op) => UnaryScalarTransformation(tpS(s), op)
        case CellArrayReferenceScalar(parent, index, tpe) => CellArrayReferenceScalar(tpC(parent), index, tpe)
        case x:ScalarParameter => x

      } ).asInstanceOf[ScalarRef]
    }

    def tpF(f: FunctionRef): FunctionRef = {
      tpMap.getOrElseUpdate(f,
      f match {
        case function(numP, body) => function(numP, tpExec(body))
        case x:FunctionParameter => x
        case VoidExecutable => VoidExecutable
      }).asInstanceOf[FunctionRef]
    }

    def tpC(c: CellArrayBase): CellArrayBase = {
      tpMap.getOrElseUpdate(c,
      c match {
        case CellArrayExecutable(elements) => CellArrayExecutable(elements map (tpExp _))
        case CellArrayReferenceCellArray(parent, idx, tpe) => CellArrayReferenceCellArray(tpC(parent), idx, tpe)
        case FixpointIterationCellArray(initial, update, maxIterations,
        convergenceFunc) => FixpointIterationCellArray(tpC(initial), tpF(update), tpS(maxIterations), tpF(convergenceFunc))
        case minWithIndex(m, dim) => minWithIndex(tpM(m), tpS(dim))
        case x: Placeholder => x
        case x:CellArrayParameter => x
      }).asInstanceOf[CellArrayBase]
    }


    tpExec(program)
  }

  /**
   * Optimize matrix matrix multiplication by reordering them
   * @param program
   * @return
   */
  def mmReorder(program: Executable): Executable ={
    mmMap.clear()
    mmOpMap.clear()

    def mmExec(program: Executable): Executable = {
      program match {
        case x: Placeholder => x
        case x: Parameter => x
        case x@WriteMatrix(m) => mmMap.getOrElseUpdate(x, WriteMatrix(mmM(m)))
        case x@WriteScalar(s) => mmMap.getOrElseUpdate(x,WriteScalar(mmS(s)))
        case x: WriteString => x
        case x@WriteFunction(f) => mmMap.getOrElseUpdate(x,WriteFunction(mmF(f)))
        case x@WriteCellArray(c) => mmMap.getOrElseUpdate(x,WriteCellArray(mmC(c)))
        case x@CompoundExecutable(exs) => mmMap.getOrElseUpdate(x,CompoundExecutable(exs map (mmExec _)))
        case x: ExpressionExecutable => mmExp(x)
      }
    }

    def mmExp(exp: ExpressionExecutable): ExpressionExecutable = {
      mmMap.getOrElseUpdate(exp,
        exp match {
          case x: Matrix => mmM(x)
          case x: ScalarRef => mmS(x)
          case x: CellArrayBase => mmC(x)
          case x: FunctionRef => mmF(x)
          case x: StringRef => x
        }).asInstanceOf[ExpressionExecutable]
    }

    def mmM(matrix: Matrix): Matrix = {
      mmMap.getOrElseUpdate(matrix,
        matrix match {
          case ScalarMatrixTransformation(s, m, op) => ScalarMatrixTransformation(mmS(s),
            mmM(m), op)
          case MatrixScalarTransformation(m,s,op) => ScalarMatrixTransformation(mmS(s),mmM(m),  op)
          case op@MatrixMult(a,b) =>
            val operands = retrieveOperands(op)
            bestMMOrder(operands)._2
          case CellArrayReferenceMatrix(parent, idx, tpe) => CellArrayReferenceMatrix(mmC(parent), idx, tpe)
          case CellwiseMatrixMatrixTransformation(a,b,op) => CellwiseMatrixMatrixTransformation(mmM(a), mmM(b), op)
          case CellwiseMatrixTransformation(a,op) => CellwiseMatrixTransformation(mmM(a), op)
          case x: Placeholder => x
          case x:LoadMatrix => x
          case x:MatrixParameter => x
          case FixpointIterationMatrix(initial, update, maxIterations, convergence) => FixpointIterationMatrix(mmM
            (initial), mmF(update), mmS(maxIterations), mmF(convergence))
          case TypeConversionMatrix(m, srcType, targetType) => TypeConversionMatrix(mmM(m), srcType, targetType)
          case VectorwiseMatrixTransformation(m, op) => VectorwiseMatrixTransformation(mmM(m), op)
          case diag(m) => diag(mmM(m))
          case eye(rows, cols) => eye(mmS(rows), mmS(cols))
          case linspace(start, end, num) => linspace(mmS(start), mmS(end), mmS(num))
          case ones(rows, cols) => ones(mmS(rows), mmS(cols))
          case randn(rows, cols, mean, std) => randn(mmS(rows), mmS(cols), mmS(mean), mmS(std))
          case pdist2(a,b) => pdist2(mmM(a), mmM(b))
          case repmat(m, a,b) => repmat(mmM(m), mmS(a), mmS(b))
          case spones(m) => spones(mmM(m))
          case sum(m,dim) => sum(mmM(m), mmS(dim))
          case sumCol(m) => sumCol(mmM(m))
          case sumRow(m) => sumRow(mmM(m))
          case zeros(rows, cols) => zeros(mmS(rows), mmS(cols))
          case Transpose(m) => Transpose(mmM(m))
        }).asInstanceOf[Matrix]
    }

    def mmS(s: ScalarRef): ScalarRef = {
      mmMap.getOrElseUpdate(s,
        s match {
          case x: scalar => x
          case b: boolean => b
          case AggregateMatrixTransformation(m, op) => AggregateMatrixTransformation(mmM(m),
            op)
          case ScalarScalarTransformation(a,b,op)=> ScalarScalarTransformation(mmS(a), mmS(b), op)
          case TypeConversionScalar(s, srcType, targetType) => TypeConversionScalar(mmS(s), srcType, targetType)
          case UnaryScalarTransformation(s, op) => UnaryScalarTransformation(mmS(s), op)
          case CellArrayReferenceScalar(parent, index, tpe) => CellArrayReferenceScalar(mmC(parent), index, tpe)
          case x:ScalarParameter => x

        } ).asInstanceOf[ScalarRef]
    }

    def mmF(f: FunctionRef): FunctionRef = {
      mmMap.getOrElseUpdate(f,
        f match {
          case function(numP, body) => function(numP, mmExec(body))
          case x:FunctionParameter => x
          case VoidExecutable => VoidExecutable
        }).asInstanceOf[FunctionRef]
    }

    def mmC(c: CellArrayBase): CellArrayBase = {
      mmMap.getOrElseUpdate(c,
        c match {
          case CellArrayExecutable(elements) => CellArrayExecutable(elements map (mmExp _))
          case CellArrayReferenceCellArray(parent, idx, tpe) => CellArrayReferenceCellArray(mmC(parent), idx, tpe)
          case FixpointIterationCellArray(initial, update, maxIterations,
          convergenceFunc) => FixpointIterationCellArray(mmC(initial), mmF(update), mmS(maxIterations), mmF(convergenceFunc))
          case minWithIndex(m, dim) => minWithIndex(mmM(m), mmS(dim))
          case x: Placeholder => x
          case x:CellArrayParameter => x
        }).asInstanceOf[CellArrayBase]
    }

    def retrieveOperands(m: Matrix): List[Matrix] = {
      m match {
        case MatrixMult(a,b) => retrieveOperands(a) ::: retrieveOperands(b)
        case m: Matrix => List(mmM(m))
      }
    }

    def bestMMOrder(operands: List[Matrix]): (Option[Int], Matrix) = {
      operands match {
        case m :: Nil => (Some(0), m)
        case a::b::Nil =>
          val m = mmOpMap.getOrElseUpdate((a,b), MatrixMult(a,b))
          (m.size, m)
        case l =>
          val splittings = for(i <- 1 until l.size) yield {
            val (left, right) = l.splitAt(i)
            val (ls, lm) = bestMMOrder(left)
            val (rs, rm) = bestMMOrder(right)
            val m = mmOpMap.getOrElseUpdate((lm,rm), MatrixMult(lm, rm))
            (maxSize(m.size,maxSize(ls,rs)), m)
          }

          splittings.reduce{
            (left, right) =>
              (left._1, right._1) match {
                case (Some(l), Some(r)) =>
                  if(l < r){
                    left
                  }else{
                    right
                  }
                case (None, Some(r)) => left
                case _ => right
              }
          }
      }
    }

    def maxSize(a: Option[Int], b: Option[Int]): Option[Int] = {
      (a,b) match {
        case (Some(aValue), Some(bValue)) => Some(math.max(aValue, bValue))
        case _ => None
      }
    }
    mmExec(program)
  }
}
