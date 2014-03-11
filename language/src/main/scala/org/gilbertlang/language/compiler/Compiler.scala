/**
 * gilbert - Distributed Linear Algebra on Sparse Matrices
 * Copyright (C) 2013  Sebastian Schelter, Till Rohrmann
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gilbertlang.language.compiler

import org.gilbertlang.runtime.Executables._
import org.gilbertlang.runtime.Operations._
import org.gilbertlang.runtime.RuntimeTypes
import scala.Some
import org.gilbertlang.language.definition.TypedAst._
import org.gilbertlang.language.definition.BuiltinSymbols
import org.gilbertlang.language.definition.Types._
import org.gilbertlang.language.definition.Operators._
import org.gilbertlang.language.definition.TypedAst
import scala.language.postfixOps

trait Compiler {
  
  val assignments = scala.collection.mutable.Map[String, ExpressionExecutable]()
  val functions = scala.collection.mutable.Map[String, TypedFunction]()

  private def addParameter(id: String, datatype: Type, position: Int) {

    datatype match {
      case _: NumericType => assignments.update(id, ScalarParameter(position))
      case _: MatrixType => assignments.update(id, MatrixParameter(position))
      case StringType => assignments.update(id, StringParameter(position))
      case _: FunctionType => assignments.update(id, FunctionParameter(position))
      case cellArray : CellArrayType =>
        val cellArrayType = createCellArrayRuntimeType(cellArray)
        assignments.update(id, CellArrayParameter(position, cellArrayType))
      case _ => throw new ParameterInsertionError("Cannot insert parameter of type " + datatype)
    }
  }
  private def createCellArrayRuntimeType(x: CellArrayType): RuntimeTypes.CellArrayType = {
    RuntimeTypes.CellArrayType(x.types map { createRuntimeType })
  }

  private def createRuntimeType(x: Type): RuntimeTypes.RuntimeType = {
    x match {
      case m: MatrixType => RuntimeTypes.MatrixType(createRuntimeType(m.elementType))
      case _: NumericType => RuntimeTypes.ScalarType
      case StringType => RuntimeTypes.StringType
      case f: FunctionType => RuntimeTypes.FunctionType
      case cellArray: CellArrayType => RuntimeTypes.CellArrayType(cellArray.types map (createRuntimeType))
    }
  }

  private def registerFunction(functionName: String, function: TypedFunction) {
    functions.update(functionName, function)
  }

  private def retrieveFunction(functionName: String) = {
    functions.get(functionName)
  }

  private def assign(identifier: String, executable: ExpressionExecutable) {
    assignments.update(identifier, executable)
  }

  private def retrieveExecutable(id: String): ExpressionExecutable = {
    assignments.getOrElse(id, VoidExecutable)
  }

  def compile(typedProgram: TypedProgram): Executable = {
    typedProgram match {
      case TypedProgram(statementsOrFunctions) => {
        val functions = statementsOrFunctions collect { case x: TypedFunction => x }
        val statements = statementsOrFunctions collect { case x: TypedStatement => x }
        functions foreach { function => registerFunction(function.identifier.value, function) }
        CompoundExecutable(statements flatMap { compileStatementWithResult })
      }
    }
  }

  def compileExpression(typedExpression: TypedExpression): ExpressionExecutable = {
    typedExpression match {
      case x: TypedIdentifier => compileIdentifier(x)
      case x: TypedInteger => scalar(x.value)
      case x: TypedFloatingPoint => scalar(x.value)
      case x: TypedString => string(x.value)
      case x: TypedBoolean => boolean(x.value)
      case x: TypedUnaryExpression => compileUnaryExpression(x)
      case x: TypedBinaryExpression => compileBinaryExpression(x)
      case x: TypedFunctionApplication => compileFunctionApplication(x)
      case x: TypedAnonymousFunction => compileAnonymousFunction(x)
      case x: TypedFunctionReference => compileFunctionReference(x)
      case x: TypedMatrix => compileMatrix(x)
      case x: TypedCellExpression => compileCellExpression(x)
    }
  }

  def compileCellExpression(cellExpression: TypedCellExpression): ExpressionExecutable = {
    cellExpression match {
      case x: TypedCellArray => compileCellArray(x)
      case x: TypedCellArrayIndexing => compileCellArrayIndexing(x)
    }
  }

  def compileCellArray(cellArray: TypedCellArray): ExpressionExecutable = {
    CellArrayExecutable(cellArray.elements map { compileExpression })
  }

  def compileCellArrayIndexing(cellArrayIndexing: TypedCellArrayIndexing): ExpressionExecutable = {
    val compiledCellArray = compileExpression(cellArrayIndexing.cellArray)

    compiledCellArray match {
      case x: CellArrayBase => x.elements(cellArrayIndexing.index.value)
      case _ => throw new CompileError("Cell array indexing requires cell array.")
    }
  }

  def compileMatrix(matrix: TypedMatrix): ExpressionExecutable = {
    VoidExecutable
  }

  def compileAnonymousFunction(anonymousFunction: TypedAnonymousFunction): ExpressionExecutable = {
    val oldAssignments = anonymousFunction.parameters map {
      x => retrieveExecutable(x.value) match {
        case VoidExecutable => None
        case y => Some(x.value, y)
      }
    }

    (anonymousFunction.parameters zipWithIndex) foreach { case (parameter, idx) => {
      addParameter(parameter.value, parameter.datatype, idx)
    }}

    val compiledBody = compileExpression(anonymousFunction.body)

    oldAssignments foreach { case Some((id, value)) => assign(id,value) case _ => }

    function(anonymousFunction.parameters.length, compiledBody)
  }

  def compileFunctionReference(functionReference: TypedFunctionReference): ExpressionExecutable = {
    compileIdentifier(functionReference.reference)
  }

  def compileIdentifier(identifier: TypedIdentifier): ExpressionExecutable = {
    identifier match {
      case x@ TypedIdentifier(id, datatype) =>
        if (BuiltinSymbols.isSymbol(id)) {
          compileBuiltInSymbol(id, datatype)
        } else {
          retrieveExecutable(id)
        }
    }
  }

  // TODO: Support of scalar values as well
  def compileBuiltInSymbol(symbol: String, datatype: Type): ExpressionExecutable = {
    symbol match {
      case "load" => function(3, LoadMatrix(StringParameter(0), ScalarParameter(1), ScalarParameter(2)))
      case "ones" => function(2, ones(ScalarParameter(0), ScalarParameter(1)))
      case "rand" => function(4, randn(ScalarParameter(0), ScalarParameter(1), ScalarParameter(2), ScalarParameter(3)))
      case "zeros" => function(2, zeros(ScalarParameter(0), ScalarParameter(1)))
      case "eye" => function(2, eye(ScalarParameter(0), ScalarParameter(1)))
      case "binarize" => {
        datatype match {
          case FunctionType(List(_: MatrixType), _) => {
            function(1, CellwiseMatrixTransformation(MatrixParameter(0), Binarize))
          }
          case FunctionType(List(_: NumericType), _) => {
            function(1, UnaryScalarTransformation(ScalarParameter(0), Binarize))
          }
        }
      }

      case "maxValue" => {
        datatype match {
          case FunctionType(List(_: MatrixType), _) => {
            function(1, AggregateMatrixTransformation(MatrixParameter(0), Maximum))
          }
          case FunctionType(List(_: NumericType, _: NumericType), _) => {
            function(2, ScalarScalarTransformation(ScalarParameter(0), ScalarParameter(1), Maximum))
          }
        }
      }

      case "fixpoint" => {
        datatype match {
          case FunctionType(List(_:MatrixType,_,_,_), _) => function(4, FixpointIteration(MatrixParameter(0),
            FunctionParameter(1),
            ScalarParameter(2), FunctionParameter(3)))
          case FunctionType(List(x:CellArrayType,_,_,_), _) =>
            function(4, FixpointIterationCellArray(CellArrayParameter(0,
            createCellArrayRuntimeType(x)), FunctionParameter(1),
          ScalarParameter(2), FunctionParameter(3)))
        }

      }

      case "spones" => {
        function(1, spones(MatrixParameter(0)))
      }

      case "sum" => {
        function(2, sum(MatrixParameter(0), ScalarParameter(1)))
      }

      case "sumRow" => {
        //function(2, sumRow(MatrixParameter(0), ScalarParameter(1)))
        function(2, sumRow(MatrixParameter(0)))
      }

      case "sumCol" => {
        //function(2, sumCol(MatrixParameter(0), ScalarParameter(1)))
        function(2, sumCol(MatrixParameter(0)))
      }

      case "diag" => {
        function(1, diag(MatrixParameter(0)))
      }

      case "write" => {
        datatype match {
          case FunctionType(List(_: MatrixType, StringType), _) => function(1, WriteMatrix(MatrixParameter(0)))
          case FunctionType(List(_: NumericType, StringType), _) => function(1, WriteScalar(ScalarParameter(0)))
          case FunctionType(List(StringType, StringType), _) => function(1, WriteString(StringParameter(0)))
        }
      }

      case "norm" => {
        function(2, norm(MatrixParameter(0), ScalarParameter(1)))
      }
    }
  }

  def compileUnaryExpression(unaryExpression: TypedUnaryExpression) = {
    val exec = compileExpression(unaryExpression.expression)

    exec match {
      case x: Matrix => {
        unaryExpression.operator match {
          case PrePlusOp => exec
          case PreMinusOp => CellwiseMatrixTransformation(x, Minus)
          case TransposeOp | CellwiseTransposeOp => Transpose(x)
        }
      }
      case x: ScalarRef => {
        unaryExpression.operator match {
          case PrePlusOp => exec
          case PreMinusOp => UnaryScalarTransformation(x, Minus)
          case TransposeOp | CellwiseTransposeOp => x
        }
      }
      case x: StringRef => throw new NotImplementedError("Unary operation of string is not yet implemented")
      case _: FunctionRef => throw new CompileError("Unary operations on functions are not supported")
      case VoidExecutable => throw new CompileError("Unary operations on VoidExecutable are not supported")
      case _: CellArrayExecutable => throw new CompileError("Unary operations on cell array are not supported")
    }
  }

  def compileBinaryExpression(binaryExpression: TypedBinaryExpression) = {
    val a = compileExpression(binaryExpression.leftExpression)
    val b = compileExpression(binaryExpression.rightExpression)

    (a,b) match {
      case (x:Matrix, y:Matrix) =>{
        binaryExpression.operator match{
          case MultOp => MatrixMult(x,y)
          case DivOp | CellwiseDivOp => CellwiseMatrixMatrixTransformation(x,y,Division)
          case PlusOp => CellwiseMatrixMatrixTransformation(x,y,Addition)
          case MinusOp => CellwiseMatrixMatrixTransformation(x,y,Subtraction)
          case CellwiseMultOp => CellwiseMatrixMatrixTransformation(x,y,Multiplication)
          case GTOp => CellwiseMatrixMatrixTransformation(x,y, GreaterThan)
          case GTEOp => CellwiseMatrixMatrixTransformation(x,y, GreaterEqualThan)
          case LTOp => CellwiseMatrixMatrixTransformation(x,y, LessThan)
          case LTEOp => CellwiseMatrixMatrixTransformation(x,y, LessEqualThan)
          case DEQOp => CellwiseMatrixMatrixTransformation(x,y, Equals)
          case NEQOp => CellwiseMatrixMatrixTransformation(x,y, NotEquals)
          case LogicalAndOp => CellwiseMatrixMatrixTransformation(x,y, And)
          case LogicalOrOp => CellwiseMatrixMatrixTransformation(x,y,Or)
          case ShortCircuitLogicalAndOp => CellwiseMatrixMatrixTransformation(x,y,SCAnd)
          case ShortCircuitLogicalOrOp => CellwiseMatrixMatrixTransformation(x,y,SCOr)
          case ExpOp | CellwiseExpOp  => {
            throw new NotImplementedError("Operator " + binaryExpression.operator + " is not yet implemented")
          }
        }
      }
      case (x:Matrix, y: ScalarRef) => {
        binaryExpression.operator match{
          case MultOp | CellwiseMultOp => MatrixScalarTransformation(x,y,Multiplication)
          case DivOp | CellwiseDivOp => MatrixScalarTransformation(x,y,Division)
          case PlusOp => MatrixScalarTransformation(x,y,Addition)
          case MinusOp => MatrixScalarTransformation(x,y,Subtraction)
          case GTOp => MatrixScalarTransformation(x,y, GreaterThan)
          case GTEOp => MatrixScalarTransformation(x,y, GreaterEqualThan)
          case LTOp => MatrixScalarTransformation(x,y, LessThan)
          case LTEOp => MatrixScalarTransformation(x,y, LessEqualThan)
          case DEQOp => MatrixScalarTransformation(x,y, Equals)
          case NEQOp => MatrixScalarTransformation(x,y, NotEquals)
          case LogicalAndOp => MatrixScalarTransformation(x,y, And)
          case LogicalOrOp => MatrixScalarTransformation(x,y,Or)
          case ShortCircuitLogicalAndOp => MatrixScalarTransformation(x,y, SCAnd)
          case ShortCircuitLogicalOrOp => MatrixScalarTransformation(x,y,SCOr)
          case ExpOp | CellwiseExpOp => {
            throw new NotImplementedError("Operator " + binaryExpression.operator + " is not yet implemented")
          }
        }
      }
      case (x:ScalarRef, y: Matrix) => {
        binaryExpression.operator match{
          case MultOp | CellwiseMultOp => ScalarMatrixTransformation(x,y,Multiplication)
          case DivOp | CellwiseDivOp => ScalarMatrixTransformation(x,y,Division)
          case PlusOp => ScalarMatrixTransformation(x,y,Addition)
          case MinusOp => ScalarMatrixTransformation(x,y,Subtraction)
          case GTOp => ScalarMatrixTransformation(x,y, GreaterThan)
          case GTEOp => ScalarMatrixTransformation(x,y, GreaterEqualThan)
          case LTOp => ScalarMatrixTransformation(x,y, LessThan)
          case LTEOp => ScalarMatrixTransformation(x,y, LessEqualThan)
          case DEQOp => ScalarMatrixTransformation(x,y, Equals)
          case NEQOp => ScalarMatrixTransformation(x,y, NotEquals)
          case LogicalAndOp => ScalarMatrixTransformation(x,y, And)
          case LogicalOrOp => ScalarMatrixTransformation(x,y,Or)
          case ShortCircuitLogicalAndOp => ScalarMatrixTransformation(x,y, SCAnd)
          case ShortCircuitLogicalOrOp => ScalarMatrixTransformation(x,y, SCOr)
          case ExpOp | CellwiseExpOp => {
            throw new NotImplementedError("Operator " + binaryExpression.operator + " is not yet implemented")
          }
        }
      }
      case (x: ScalarRef, y: ScalarRef) => {
        binaryExpression.operator match{
          case MultOp | CellwiseMultOp => ScalarScalarTransformation(x,y,Multiplication)
          case DivOp | CellwiseDivOp => ScalarScalarTransformation(x,y,Division)
          case PlusOp => ScalarScalarTransformation(x,y,Addition)
          case MinusOp => ScalarScalarTransformation(x,y,Subtraction)
          case GTOp => ScalarScalarTransformation(x,y, GreaterThan)
          case GTEOp => ScalarScalarTransformation(x,y, GreaterEqualThan)
          case LTOp => ScalarScalarTransformation(x,y, LessThan)
          case LTEOp => ScalarScalarTransformation(x,y, LessEqualThan)
          case DEQOp => ScalarScalarTransformation(x,y, Equals)
          case NEQOp => ScalarScalarTransformation(x,y, NotEquals)
          case LogicalAndOp => ScalarScalarTransformation(x,y, And)
          case LogicalOrOp => ScalarScalarTransformation(x,y,Or)
          case ShortCircuitLogicalAndOp => ScalarScalarTransformation(x,y, SCAnd)
          case ShortCircuitLogicalOrOp => ScalarScalarTransformation(x,y,SCOr)
          case ExpOp | CellwiseExpOp => {
            throw new NotImplementedError("Operator " + binaryExpression.operator + " is not yet implemented")
          }
        }
      }
      case (x: StringRef, y: StringRef) => {
        throw new NotImplementedError("Binary operation for 2 strings is not yet implemented")
      }
      case (x: StringRef, y: Matrix) => {
        throw new NotImplementedError("Binary operation for string and matrix is not supported")
      }
      case (x: StringRef, y: ScalarRef) => {
        throw new NotImplementedError("Binary operation for string and scalar is not yet implemented")
      }
      case (x: Matrix, y: StringRef) => {
        throw new NotImplementedError("BinaryOperation for matrix and string is not supported")
      }
      case (x: ScalarRef, y: StringRef) => {
        throw new NotImplementedError("BinaryOperation for scalar and string is not supported")
      }
      case (_: FunctionRef, _) | (_, _: FunctionRef) => {
        throw new CompileError("Binary operation on a function is not supported")
      }
      case (VoidExecutable, _) | (_ , VoidExecutable) => {
        throw new CompileError("Binary operation on VoidExecutable is not supported")
      }
    }

  }

  def compileFunctionApplication(functionApplication: TypedFunctionApplication) = {
    val fun = compileIdentifier(functionApplication.id)

    val result = fun match {
      case function(numParameters, body) if numParameters <= functionApplication.args.length =>
        val arguments = functionApplication.args map { compileExpression }
        body.instantiate(arguments:_*) match {
        case x:ExpressionExecutable => x
        case _ => throw new TypeCompileError("Return value of a function has to be an expression")
      }
      case VoidExecutable =>
        compileFunctionDefinition(functionApplication)
      case _ => throw new TypeCompileError("Id has to be of a function type")
    }
    
    result
  }

  def compileFunctionDefinition(functionApplication: TypedFunctionApplication): ExpressionExecutable = {
    val compiler = new Compiler {}
    val typedFunction = retrieveFunction(functionApplication.id.value) match {
      case Some(func) => func
      case None => throw new CompileError("Function " + functionApplication.id.value + " is not registered.")
    }

    val typedArguments = functionApplication.args map { compileExpression }

    assignments foreach { case (id, value) => compiler.assign(id, value) }

    if(typedArguments.length >= typedFunction.parameters.length){
      typedFunction.parameters zip typedArguments map { case (parameter, argument) => compiler.assign(parameter
        .value, argument)}

      compiler.compile(typedFunction.body)

      val results = typedFunction.values map { result => compiler.retrieveExecutable(result.value) }
      results(0)
    }else{
      throw new CompileError("Function application requires more parameters than there are arguments provided.")
    }
  }

  def compileStatementWithResult(typedStatement: TypedStatement): Option[Executable] = {
    typedStatement match {
      case TypedAssignment(lhs, rhs) => {
        val result = compileExpression(rhs)
        assign(lhs.value, result)
        None
      }
      case x: TypedExpression => {
        compileExpression(x)
        None
      }
      case TypedNOP => None
      case TypedOutputResultStatement(stmt) => {
        TypedAst.getType(stmt) match {
          case _: MatrixType => {
            compileStatement(stmt) match {
              case x: Matrix => Some(WriteMatrix(x))
              case _ => throw new TypeCompileError("Expected executable of type Matrix")
            }
          }
          case _: NumericType => {
            compileStatement(stmt) match {
              case x: ScalarRef => Some(WriteScalar(x))
              case _ => throw new TypeCompileError("Expected executable of type ScalarRef")
            }
          }

          case BooleanType => {
            compileStatement(stmt) match {
              case x: ScalarRef => Some(WriteScalar(x))
              case _ => throw new TypeCompileError("Expected executable of type ScalarRef")
            }
          }

          case StringType => {
            compileStatement(stmt) match {
              case x: StringRef => Some(WriteString(x))
              case _ => throw new TypeCompileError("Expected executable of type StringRef")
            }
          }

          case _: CellArrayType => {
            compileStatement(stmt) match {
              case x: CellArrayBase => Some(WriteCellArray(x))
              case _ => throw new TypeCompileError("Expected executable of type CellArrayBase")
            }
          }

          case _: FunctionType => {
            compileStatement(stmt) match {
              case x: FunctionRef => Some(WriteFunction(x))
              case _ => throw new TypeCompileError("Expected executable of type FunctionRef")
            }
          }

          case tpe => throw new TypeCompileError("Cannot output type " + tpe)
        }
      }
    }
  }

  def compileStatement(typedStatement: TypedStatement): Executable = {
    typedStatement match {
      case TypedAssignment(lhs, rhs) => {
        val result = compileExpression(rhs)
        assign(lhs.value, result)
        result
      }
      case x: TypedExpression => compileExpression(x)
      case TypedNOP => VoidExecutable
      case TypedOutputResultStatement(stmt) => {
        TypedAst.getType(stmt) match {
          case _: MatrixType =>
            compileStatement(stmt) match {
              case x: Matrix => WriteMatrix(x)
              case _ => throw new TypeCompileError("Expected executable of type Matrix")
            }
          case _: NumericType =>
            compileStatement(stmt) match {
              case x: ScalarRef => WriteScalar(x)
              case _ => throw new TypeCompileError("Expected executable of type ScalarRef")
            }
          case StringType => {
            compileStatement(stmt) match {
              case x: StringRef => WriteString(x)
              case _ => throw new TypeCompileError("Expected executable of type StringRef")
            }
          }
          case _ => VoidExecutable
        }
      }
    }
  }
}