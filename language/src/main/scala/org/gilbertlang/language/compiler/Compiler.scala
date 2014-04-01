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
import scala.Some
import org.gilbertlang.language.definition.{TypedAbstractSyntaxTree, Values, BuiltinSymbols}
import org.gilbertlang.language.definition.Types._
import org.gilbertlang.language.definition.Operators._
import scala.language.postfixOps
import org.gilbertlang.runtime.RuntimeTypes
import org.gilbertlang.language.definition.TypedAbstractSyntaxTree._

trait Compiler {
  
  val assignments = scala.collection.mutable.Map[String, ExpressionExecutable]()
  val functions = scala.collection.mutable.Map[String, TypedFunction]()

  private def addParameter(id: String, datatype: Type, position: Int) {

    datatype match {
      case BooleanType => assignments.update(id, ScalarParameter(position))
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
      case m: MatrixType => RuntimeTypes.MatrixType(createRuntimeType(m.elementType), Values.value2Int(m.rows),
        Values.value2Int(m.columns))
      case _: NumericType => RuntimeTypes.DoubleType
      case BooleanType => RuntimeTypes.BooleanType
      case StringType => RuntimeTypes.StringType
      case f: FunctionType => RuntimeTypes.FunctionType
      case cellArray: CellArrayType => RuntimeTypes.CellArrayType(cellArray.types map createRuntimeType)
    }
  }

  private def createTypeSuffix(parameters: List[Type]): String = {
    "$" + (parameters map { createTypeSuffix } mkString "")
  }

  private def createTypeSuffix(tpe: Type): String = {
    tpe match {
      case StringType => "S"
      case BooleanType => "B"
      case CharacterType => "C"
      case DoubleType => "D"
      case VoidType => "V"
      case MatrixType(elementTpe, _, _) => "M"
      case cell: CellArrayType => "C"
      case FunctionType(parameters, result) => "F"
      case _: AbstractTypeVar => throw new CompileError("Create type suffix requires concrete types.")
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
      case TypedProgram(statementsOrFunctions) =>
        compile(statementsOrFunctions)
    }
  }

  def compile(statementsOrFunctions: List[TypedStatementOrFunction]): Executable = {
    val functions = statementsOrFunctions collect { case x: TypedFunction => x }
    val statements = statementsOrFunctions collect { case x: TypedStatement => x }
    functions foreach { function => registerFunction(function.identifier.value, function) }
    val executables = statements flatMap compileStatementWithResult
    CompoundExecutable(executables)
  }

  def compileExpression(typedExpression: TypedExpression): ExpressionExecutable = {
    typedExpression match {
      case x: TypedIdentifier => compileIdentifier(x)
      case x: TypedNumericLiteral => scalar(x.value)
      case x: TypedString => string(x.value)
      case x: TypedBoolean => boolean(x.value)
      case x: TypedUnaryExpression => compileUnaryExpression(x)
      case x: TypedBinaryExpression => compileBinaryExpression(x)
      case x: TypedFunctionApplication => compileFunctionApplication(x)
      case x: TypedAnonymousFunction => compileAnonymousFunction(x)
      case x: TypedFunctionReference => compileFunctionReference(x)
      case x: TypedMatrix => compileMatrix(x)
      case x: TypedCellExpression => compileCellExpression(x)
      case x: TypeConversion => compileTypeConversion(x)
    }
  }

  def compileTypeConversion(typeConversion: TypeConversion): ExpressionExecutable = {
    val sourceType = createRuntimeType(typeConversion.expression.datatype)
    val targetType = createRuntimeType(typeConversion.datatype)

    val compiledExpression = compileExpression(typeConversion.expression)
    compiledExpression match {
      case x: ScalarRef =>

        TypeConversionScalar(x, sourceType.asInstanceOf[RuntimeTypes.ScalarType],
          targetType.asInstanceOf[RuntimeTypes.ScalarType])
      case x: Matrix =>
        TypeConversionMatrix(x, sourceType.asInstanceOf[RuntimeTypes.MatrixType],
          targetType.asInstanceOf[RuntimeTypes.MatrixType])
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
    val index = cellArrayIndexing.index
    compiledCellArray match {
      case x: CellArrayBase => x(index)
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

    (anonymousFunction.parameters zipWithIndex) foreach { case (parameter, idx) =>
      addParameter(parameter.value, parameter.datatype, idx)
    }

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
        datatype match {
          case FunctionType(parameters, value) =>
            val typeSuffix = createTypeSuffix(parameters)

            if(BuiltinSymbols.isSymbol(id)){
              compileBuiltInSymbol(id+typeSuffix, datatype)
            }else{
              val result = retrieveExecutable(id+typeSuffix)

              result match {
                case VoidExecutable =>
                  val instantiatedFunction = instantiateFunctionDefinition(id, parameters)
                  instantiatedFunction match {
                    case VoidExecutable => VoidExecutable
                    case x =>
                      assign(id+typeSuffix, instantiatedFunction)
                      instantiatedFunction
                  }
                case x => x
              }
            }
          case _ =>
            if (BuiltinSymbols.isSymbol(id)) {
              compileBuiltInSymbol(id, datatype)
            } else {
              retrieveExecutable(id)
            }
        }
    }
  }

  // TODO: Support of scalar values as well
  def compileBuiltInSymbol(symbol: String, datatype: Type): ExpressionExecutable = {
    symbol match {
      case "load$SDD" => function(3, LoadMatrix(StringParameter(0), ScalarParameter(1), ScalarParameter(2)))
      case "repmat$MDD" => function(3, repmat(MatrixParameter(0), ScalarParameter(1), ScalarParameter(2)))
      case "linspace$DDD" => function(3, linspace(ScalarParameter(0), ScalarParameter(1), ScalarParameter(2)))
      case "pdist2$MM" => function(2, pdist2(MatrixParameter(0), MatrixParameter(1)))
      case "minWithIndex$MD" => function(2, minWithIndex(MatrixParameter(0), ScalarParameter(1)))
      case "ones$DD" => function(2, ones(ScalarParameter(0), ScalarParameter(1)))
      case "rand$DDDD" => function(4, randn(ScalarParameter(0), ScalarParameter(1), ScalarParameter(2),
        ScalarParameter(3)))
      case "zeros$DD" => function(2, zeros(ScalarParameter(0), ScalarParameter(1)))
      case "eye$DD" => function(2, eye(ScalarParameter(0), ScalarParameter(1)))
      case "binarize$M" => function(1, CellwiseMatrixTransformation(MatrixParameter(0), Binarize))
      case "binarize$D" => function(1, UnaryScalarTransformation(ScalarParameter(0), Binarize))
      case "maxValue$M" => function(1, AggregateMatrixTransformation(MatrixParameter(0), Maximum))
      case "maxValue$D" => function(2, ScalarScalarTransformation(ScalarParameter(0), ScalarParameter(1), Maximum))

      case "fixpoint$MFDF" => function(4, FixpointIteration(MatrixParameter(0),
        FunctionParameter(1), ScalarParameter(2), FunctionParameter(3)))

      case "fixpoint$MFD" => function(3, FixpointIteration(MatrixParameter(0), FunctionParameter(1),
        ScalarParameter(2), null))

      case "fixpoint$CFDF" =>
        datatype match {
          case FunctionType(List(x:CellArrayType,_,_,_), _) => function(4, FixpointIterationCellArray(
            CellArrayParameter(0, createCellArrayRuntimeType(x)), FunctionParameter(1),ScalarParameter(2),
            FunctionParameter(3)))
        }

      case "fixpoint$CFD" =>
        datatype match {
          case FunctionType(List(x:CellArrayType, _, _), _) => function(3,
            FixpointIterationCellArray( CellArrayParameter(0, createCellArrayRuntimeType(x)), FunctionParameter(1),
              ScalarParameter(2), null))
        }


      case "spones$M" =>
        function(1, spones(MatrixParameter(0)))

      case "sum$MD" =>
        function(2, sum(MatrixParameter(0), ScalarParameter(1)))

      case "sumRow$M" =>
        //function(2, sumRow(MatrixParameter(0), ScalarParameter(1)))
        function(2, sumRow(MatrixParameter(0)))

      case "sumCol$M" =>
        //function(2, sumCol(MatrixParameter(0), ScalarParameter(1)))
        function(2, sumCol(MatrixParameter(0)))

      case "diag$M" =>
        function(1, diag(MatrixParameter(0)))

      case "write$MS" => function(1, WriteMatrix(MatrixParameter(0)))
      case "write$DS" => function(1, WriteScalar(ScalarParameter(0)))
      case "write$SS" => function(1, WriteString(StringParameter(0)))

      case "norm$MD" =>
        function(2, norm(MatrixParameter(0), ScalarParameter(1)))
    }
  }

  def compileUnaryExpression(unaryExpression: TypedUnaryExpression) = {
    val exec = compileExpression(unaryExpression.expression)

    exec match {
      case x: Matrix =>
        unaryExpression.operator match {
          case PrePlusOp => exec
          case PreMinusOp => CellwiseMatrixTransformation(x, Minus)
          case TransposeOp | CellwiseTransposeOp => Transpose(x)
        }
      case x: ScalarRef =>
        unaryExpression.operator match {
          case PrePlusOp => exec
          case PreMinusOp => UnaryScalarTransformation(x, Minus)
          case TransposeOp | CellwiseTransposeOp => x
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
      case (x:Matrix, y:Matrix) =>
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
          case ExpOp | CellwiseExpOp  =>
            throw new NotImplementedError("Operator " + binaryExpression.operator + " is not yet implemented")
        }
      case (x:Matrix, y: ScalarRef) =>
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
          case ExpOp | CellwiseExpOp =>
            throw new NotImplementedError("Operator " + binaryExpression.operator + " is not yet implemented")
        }
      case (x:ScalarRef, y: Matrix) =>
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
          case ExpOp | CellwiseExpOp =>
            throw new NotImplementedError("Operator " + binaryExpression.operator + " is not yet implemented")
        }
      case (x: ScalarRef, y: ScalarRef) =>
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
          case ExpOp | CellwiseExpOp =>
            throw new NotImplementedError("Operator " + binaryExpression.operator + " is not yet implemented")
        }
      case (x: StringRef, y: StringRef) =>
        throw new NotImplementedError("Binary operation for 2 strings is not yet implemented")
      case (x: StringRef, y: Matrix) =>
        throw new NotImplementedError("Binary operation for string and matrix is not supported")
      case (x: StringRef, y: ScalarRef) =>
        throw new NotImplementedError("Binary operation for string and scalar is not yet implemented")
      case (x: Matrix, y: StringRef) =>
        throw new NotImplementedError("BinaryOperation for matrix and string is not supported")
      case (x: ScalarRef, y: StringRef) =>
        throw new NotImplementedError("BinaryOperation for scalar and string is not supported")
      case (_: FunctionRef, _) | (_, _: FunctionRef) =>
        throw new CompileError("Binary operation on a function is not supported")
      case (VoidExecutable, _) | (_ , VoidExecutable) =>
        throw new CompileError("Binary operation on VoidExecutable is not supported")
    }

  }

  def compileFunctionApplication(functionApplication: TypedFunctionApplication) = {
    val fun = compileExpression(functionApplication.function)

    val result = fun match {
      case function(numParameters, body) if numParameters <= functionApplication.args.length =>
        val arguments = functionApplication.args map { compileExpression }
        body.instantiate(arguments:_*) match {
        case x:ExpressionExecutable => x
        case _ => throw new TypeCompileError("Return value of a function has to be an expression")
      }
      case _ => throw new TypeCompileError("Id has to be of a function type")
    }
    
    result
  }

  def instantiateFunctionDefinition(id: String, parameterTypes: List[Type] ): ExpressionExecutable = {
    val compiler = new Compiler {}
    val typedFunction = retrieveFunction(id) match {
      case Some(func) => func
      case None => throw new CompileError("Function " + id + " is not registered.")
    }

    assignments foreach { case (id, value) => compiler.assign(id, value) }

    if( parameterTypes.length >= typedFunction.parameters.length){
      val parameterNames = typedFunction.parameters map { parameter => parameter.value }
      ((parameterNames zip parameterTypes) zipWithIndex) map { case ((id, tpe), idx) => compiler.addParameter(id,tpe,
      idx)}

      compiler.compile(typedFunction.body)


      val results = typedFunction.values map { result => compiler.retrieveExecutable(result.value) }
      function(typedFunction.parameters.length, results(0))
    }else{
      throw new CompileError("Function application requires more parameters than there are arguments provided.")
    }
  }

  def compileStatementWithResult(typedStatement: TypedStatement): Option[Executable] = {
    typedStatement match {
      case TypedAssignment(lhs, rhs) =>
        rhs match {
          case TypedAnonymousFunction(parameters, expression, _, datatype) =>
            val functionResult = TypedIdentifier("functionResult", expression.datatype)
            val assignment = TypedAssignment(functionResult, expression)
            val typedFunction = TypedFunction(List(functionResult),lhs, parameters, TypedProgram(List(assignment)))
            registerFunction(lhs.value, typedFunction)
          case TypedFunctionReference(reference,_) =>
            retrieveFunction(reference.value) match {
              case Some(func) => registerFunction(lhs.value, func)
              case None => throw new CompileError("There is no function registered for " + reference.value)
            }
          case _ =>
            val result = compileExpression(rhs)
            assign(lhs.value, result)
        }
        None
      case x: TypedExpression =>
        compileExpression(x)
        None
      case TypedNOP => None
      case TypedOutputResultStatement(stmt) =>
        TypedAbstractSyntaxTree.getType(stmt) match {
          case _: MatrixType =>
            compileStatement(stmt) match {
              case x: Matrix => Some(WriteMatrix(x))
              case _ => throw new TypeCompileError("Expected executable of type Matrix")
            }
          case _: NumericType =>
            compileStatement(stmt) match {
              case x: ScalarRef => Some(WriteScalar(x))
              case _ => throw new TypeCompileError("Expected executable of type ScalarRef")
            }

          case BooleanType =>
            compileStatement(stmt) match {
              case x: ScalarRef => Some(WriteScalar(x))
              case _ => throw new TypeCompileError("Expected executable of type ScalarRef")
            }

          case StringType =>
            compileStatement(stmt) match {
              case x: StringRef => Some(WriteString(x))
              case _ => throw new TypeCompileError("Expected executable of type StringRef")
            }

          case _: CellArrayType =>
            compileStatement(stmt) match {
              case x: CellArrayBase => Some(WriteCellArray(x))
              case _ => throw new TypeCompileError("Expected executable of type CellArrayBase")
            }

          case _: FunctionType =>
            compileStatement(stmt) match {
              case x: FunctionRef => Some(WriteFunction(x))
              case _ => throw new TypeCompileError("Expected executable of type FunctionRef")
            }

          case tpe => throw new TypeCompileError("Cannot output type " + tpe)
        }
    }
  }

  def compileStatement(typedStatement: TypedStatement): Executable = {
    typedStatement match {
      case TypedAssignment(lhs, rhs) =>
        rhs match {
          case TypedAnonymousFunction(parameters, expression, _, datatype) =>
            val functionResult = TypedIdentifier("functionResult", expression.datatype)
            val assignment = TypedAssignment(functionResult, expression)
            val typedFunction = TypedFunction(List(functionResult),lhs, parameters, TypedProgram(List(assignment)))
            registerFunction(lhs.value, typedFunction)
            VoidExecutable
          case TypedFunctionReference(reference, _) =>
            retrieveFunction(reference.value) match {
              case Some(func) =>
                registerFunction(lhs.value, func)
                VoidExecutable
              case None => throw new CompileError("There is no function registered for " + reference.value)
            }
          case _ =>
            val result = compileExpression(rhs)
            assign(lhs.value, result)
            result
        }
      case x: TypedExpression => compileExpression(x)
      case TypedNOP => VoidExecutable
      case TypedOutputResultStatement(stmt) =>
        TypedAbstractSyntaxTree.getType(stmt) match {
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
          case StringType =>
            compileStatement(stmt) match {
              case x: StringRef => WriteString(x)
              case _ => throw new TypeCompileError("Expected executable of type StringRef")
            }
          case _ => VoidExecutable
        }
    }
  }
}