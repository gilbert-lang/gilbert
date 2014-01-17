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

package org.gilbertlang.language.typer

import org.gilbertlang.language.definition.Types._
import org.gilbertlang.language.definition.TypedAst._
import org.gilbertlang.language.definition.Values._
import org.gilbertlang.language.definition.AbstractSyntaxTree._
import org.gilbertlang.language.definition.{Values, Types, BuiltinSymbols}
import org.gilbertlang.language.definition.Values.ValueVar
import scala.Some
import org.gilbertlang.language.definition.Types.NumericTypeVar
import org.gilbertlang.language.definition.Values.ReferenceValue
import org.gilbertlang.language.definition.TypedAst.TypedIdentifier
import org.gilbertlang.language.definition.Types.PolymorphicType
import org.gilbertlang.language.definition.Types.TypeVar
import org.gilbertlang.language.definition.TypedAst.TypedInteger
import org.gilbertlang.language.definition.Values.IntValue
import org.gilbertlang.language.definition.TypedAst.TypedProgram
import org.gilbertlang.language.definition.Types.UniversalType
import org.gilbertlang.language.definition.Values.UniversalValue
import org.gilbertlang.language.definition.Types.MatrixType
import org.gilbertlang.language.definition.Operators._

import org.gilbertlang.language.definition.ConvenienceMethods._

import Types.Helper._
import Values.Helper._

trait Typer {

  private val typeEnvironment = scala.collection.mutable.Map[String, Type]()
  private val valueEnvironment = scala.collection.mutable.Map[String, TypedExpression]()
  private val typeVarMapping = scala.collection.mutable.Map[Type, Type]()
  private val valueVarMapping = scala.collection.mutable.Map[Value, Value]()

  def resolveType(datatype: Type): Type = {
    datatype match {
      case _: AbstractTypeVar => {
        var result: Type = datatype

        while (result != typeVarMapping.getOrElse(result, result)) {
          result = typeVarMapping(result)
        }

        result match {
          case _: AbstractTypeVar => result
          case _ => resolveType(result)
        }
      }
      case MatrixType(elementType, rowValue, colValue) => {
        MatrixType(resolveType(elementType), resolveValue(rowValue), resolveValue(colValue))
      }
      case FunctionType(args, result) => FunctionType(args map { resolveType }, resolveType(result))
      case PolymorphicType(types) => PolymorphicType(types map { resolveType })
      case x => x
    }
  }

  def simplifyValue(value: Value) = value

  def resolveValue(value: Value): Value = {
    val resolvedValue = value match {
      case _: ValueVar => {
        var result: Value = value

        while (result != valueVarMapping.getOrElse(result, result)) {
          result = valueVarMapping(result)
        }

        result
      }
      case _ => value
    }

    simplifyValue(resolvedValue)
  }

  def updateTypeVarMapping(typeVar: AbstractTypeVar, datatype: Type) {
    typeVarMapping.update(typeVar, datatype)
  }

  def updateValueVarMapping(valueVar: ValueVar, value: Value) {
    valueVarMapping.update(valueVar, value)
  }

  def resolvePolymorphicType(a: Type, b: Type): Option[(Type, Int)] = {
    a match {
      case PolymorphicType(types) =>
        types.zipWithIndex.toIterator.map {
          case (signature, index) => unify(specializeType(generalizeType(b)), signature) match {
            case Some(_) => unify(b, signature) match {
              case Some(t) => Some(t, index)
              case _ => None
            }
            case _ => None
          }
        } find ({ x: Option[(Type, Int)] => x != None }) flatten
      case _ => unify(b, a) match {
        case Some(t) => Some(t, 0)
        case _ => None
      }
    }
  }

  def resolveValueReferences(datatype: Type, arguments: List[TypedExpression]): Type = {
    datatype match {
      case FunctionType(args, result) => {
        FunctionType(args map { resolveValueReferences(_, arguments) }, resolveValueReferences(result, arguments))
      }
      case PolymorphicType(types) => PolymorphicType(types map { resolveValueReferences(_, arguments) })
      case MatrixType(elementType, rowValue, colValue) => {
        MatrixType(resolveValueReferences(elementType, arguments), resolveValueReferences(rowValue, arguments),
          resolveValueReferences(colValue, arguments))
      }
      case x => x
    }
  }

  def evaluateExpression(expression: TypedExpression): Value = {
    expression match {
      case TypedInteger(value) => IntValue(value)
      case TypedIdentifier(id, _) => {
        getValue(id) match {
          case Some(t) => evaluateExpression(t)
          case _ => throw new ValueNotFoundError("identifier " + id + " has no value assigned")
        }
      }
      case _ => throw new NotImplementedError("expression evaluation is not yet fully implemented")
    }
  }

  def resolveValueReferences(value: Value, arguments: List[TypedExpression]): Value = {
    value match {
      case ReferenceValue(idx) => evaluateExpression(arguments(idx))
      case x => x
    }
  }

  def widenTypes(a: Type, b: Type): (Type, Type) = {
    (a.isWideableTo(b), b.isWideableTo(a)) match {
      case (true, _) => (b, b)
      case (_, true) => (a, a)
      case _ => (a, b)
    }
  }

  def specializeType(datatype: Type): Type = {
    val replacement = scala.collection.mutable.Map[AbstractTypeVar, AbstractTypeVar]()
    val replacementValues = scala.collection.mutable.Map[ValueVar, ValueVar]()
    def helper(a: Type): Type = {
      a match {
        case UniversalType(x: NumericTypeVar) => replacement.getOrElseUpdate(x, newNumericTV())
        case UniversalType(x: TypeVar) => replacement.getOrElseUpdate(x, newTV())
        case MatrixType(elementType, rowValue, colValue) => {
          MatrixType(helper(elementType), helperValue(rowValue), helperValue(colValue))
        }
        case PolymorphicType(types) => PolymorphicType(types map { helper })
        case FunctionType(args, result) => FunctionType(args map { helper }, helper(result))
        case x => x
      }
    }
    def helperValue(a: Value): Value = {
      a match {
        case UniversalValue(x: ValueVar) => replacementValues.getOrElseUpdate(x, newVV())
        case x => x
      }
    }
    helper(datatype)
  }

  def specializeValue(value: Value): Value = {
    val replacement = scala.collection.mutable.Map[ValueVar, ValueVar]()
    def helper(a: Value): Value = {
      a match {
        case UniversalValue(x: ValueVar) => replacement.getOrElseUpdate(x, newVV())
        case x => x
      }
    }
    helper(value)
  }

  def freeVariables(expression: ASTExpression): Set[String] = {
    expression match {
      case x: ASTIdentifier => freeVariables(x)
      case _: ASTInteger | _: ASTFloatingPoint | _: ASTString => Set()
      case ASTUnaryExpression(exp, _) => freeVariables(exp)
      case ASTBinaryExpression(a, op, b) => freeVariables(a) ++ freeVariables(b)
      case ASTFunctionReference(id) => freeVariables(id)
      case ASTAnonymousFunction(params, body) => {
        freeVariables(body) -- (params map { case ASTIdentifier(id) => id }).toSet
      }
      case ASTFunctionApplication(func, args) => freeVariables(func) ++ (args flatMap { freeVariables(_) }).toSet
      case ASTMatrix(rows) => (rows flatMap { freeVariables(_) }).toSet
      case ASTMatrixRow(exps) => (exps flatMap { freeVariables(_) }).toSet
    }
  }

  def freeVariables(identifier: ASTIdentifier): Set[String] = {
    identifier match {
      case ASTIdentifier(id) => {
        if (BuiltinSymbols.isSymbol(id)) {
          Set()
        } else {
          Set(id)
        }
      }
    }
  }

  def freeTypeVariables(datatype: Type): Set[Type] = {
    def helper(a: Type): Set[Type] = {
      a match {
        case UniversalType(x: AbstractTypeVar) => Set()
        case UniversalType(_) => throw new TypingError("Universal cannot be applied to a non type variable")
        case x: AbstractTypeVar => Set(x)
        case MatrixType(elementType, rowValue, colValue) => helper(elementType)
        case PolymorphicType(types) => (types flatMap (helper)).toSet
        case FunctionType(args, result) => (args flatMap (helper)).toSet ++ helper(result)
        case _ => Set()
      }
    }
    helper(datatype)
  }

  def generalizeType(datatype: Type): Type = {
    val freeVars = typeEnvironment.values.flatMap({ freeTypeVariables }).toSet
    def helper(datatype: Type): Type = {
      datatype match {
        case x @ UniversalType(_: AbstractTypeVar) => x
        case UniversalType(_) => throw new TypingError("Universal cannot be applied to a non type variable")
        case x: AbstractTypeVar => if (freeVars contains x) x else UniversalType(x)
        case MatrixType(elementType, rowValue, colValue) => {
          MatrixType(helper(elementType), generalizeValue(rowValue), generalizeValue(colValue))
        }
        case PolymorphicType(types) => PolymorphicType(types map { helper })
        case FunctionType(args, result) => FunctionType(args map { helper }, helper(result))
        case x => x
      }
    }
    helper(datatype)
  }

  def generalizeValue(value: Value): Value = {
    value match {
      case x: ValueVar => UniversalValue(x)
      case x => x
    }
  }

  def unifyValue(a: Value, b: Value): Option[Value] = {
    val resolvedValueA = resolveValue(a)
    val resolvedValueB = resolveValue(b)

    if (resolvedValueA == resolvedValueB) {
      Some(resolvedValueA)
    } else {
      (resolvedValueA, resolvedValueB) match {
        case (x: ValueVar, y) => {
          updateValueVarMapping(x, y)
          Some(y)
        }
        case (x, y: ValueVar) => {
          updateValueVarMapping(y, x)
          Some(x)
        }
        case (UndefinedValue, _) => Some(UndefinedValue)
        case (_, UndefinedValue) => Some(UndefinedValue)
        case _ => None
      }
    }
  }

  //TODO: type variable contained in other type as subexpression
  def unify(a: Type, b: Type): Option[Type] = {
    val resolvedTypeA = resolveType(a)
    val resolvedTypeB = resolveType(b)

    val (typeA, typeB) = widenTypes(resolvedTypeA, resolvedTypeB)

    if (typeA == typeB) {
      Some(typeA)
    } else {
      (typeA, typeB) match {
        case (x: TypeVar, _) => {
          updateTypeVarMapping(x, typeB)
          Some(typeB)
        }
        case (_, y: TypeVar) => {
          updateTypeVarMapping(y, typeA)
          Some(typeA)
        }
        case (x: NumericTypeVar, y: NumericType) => {
          updateTypeVarMapping(x, y)
          Some(y)
        }
        case (x: NumericType, y: NumericTypeVar) => {
          updateTypeVarMapping(y, x)
          Some(x)
        }
        case (FunctionType(args1, result1), FunctionType(args2, result2)) => {
          if (args1.length != args2.length) None
          else {
            val unifiedArgs = (for ((x, y) <- (args1 zip args2)) yield {
              unify(x, y)
            }) flatMap { x => x }
            val unifiedResult = unify(result1, result2)

            unifiedResult match {
              case Some(resultType) if unifiedArgs.length == args1.length => Some(FunctionType(unifiedArgs, resultType))
              case _ => None
            }
          }
        }
        case (MatrixType(matrixType1, rows1, cols1), MatrixType(matrixType2, rows2, cols2)) => {
          val unifiedType = unify(matrixType1, matrixType2)
          val unifiedRows = unifyValue(rows1, rows2)
          val unifiedCols = unifyValue(cols1, cols2)

          unifiedType match {
            case Some(t) => unifiedRows match {
              case Some(r) => unifiedCols match {
                case Some(c) => Some(MatrixType(t, r, c))
                case _ => None
              }
              case _ => None
            }
            case _ => None
          }
        }
        case (PolymorphicType(typesA), PolymorphicType(typesB)) => None
        case (CharacterType, _) | (_, CharacterType) => None
        case (_, _) => None
      }
    }
  }

  def extractIdentifiers(program: ASTProgram): Set[ASTIdentifier] = Set()

  def extractIdentifiers(expression: ASTExpression): Set[String] = {
    def helper(exp: ASTExpression): Set[String] = {
      exp match {
        case ASTIdentifier(id) => Set(id)
        case _: ASTInteger | _: ASTFloatingPoint | _: ASTString => Set()
        case ASTUnaryExpression(exp, _) => helper(exp)
        case ASTBinaryExpression(a, _, b) => helper(a) ++ helper(b)
        case ASTAnonymousFunction(parameters, body) => {
          helper(body) -- (parameters map { case ASTIdentifier(id) => id }).toSet
        }
        case ASTFunctionApplication(function, parameters) => {
          helper(function) ++ (parameters flatMap { helper }).toSet
        }
        case ASTFunctionReference(function) => helper(function)
        case ASTMatrix(rows) => rows flatMap { helper } toSet
        case ASTMatrixRow(exps) => exps flatMap { helper } toSet
      }
    }
    helper(expression)
  }

  def updateEnvironment(identifier: String, datatype: Type) = typeEnvironment.update(identifier, datatype)
  def updateEnvironment(identifier: ASTIdentifier, datatype: Type) = typeEnvironment.update(identifier.value, datatype)

  def updateValueEnvironment(identifier: ASTIdentifier, expression: TypedExpression): Unit = {
    updateValueEnvironment(identifier.value, expression)
  }

  def updateValueEnvironment(identifier: String, expression: TypedExpression): Unit = {
    valueEnvironment.update(identifier, expression)
  }

  def getType(id: String): Option[Type] = {
    BuiltinSymbols.getType(id) match {
      case None => typeEnvironment.get(id) match {
        case Some(t) => Some(resolveType(t))
        case None => None
      }
      case Some(t) => Some(resolveType(t))
    }
  }

  def getValue(id: String): Option[TypedExpression] = {
    valueEnvironment.get(id)
  }

  def extractType(expression: TypedExpression) = expression.datatype

  def typeProgram(program: ASTProgram): TypedProgram = program match {
    case ASTProgram(stmtFuncList) => TypedProgram(stmtFuncList map {
      case stmt: ASTStatement => typeStmt(stmt)
      case func: ASTFunction => typeFunction(func)
      case typeAnnotation: ASTTypeAnnotation =>
        throw new NotYetImplementedError("Type annotations are not yet supported")
    })
  }

  def typeStmt(stmt: ASTStatement): TypedStatement = stmt match {
    case ASTOutputResultStatement(stmt) => TypedOutputResultStatement(typeStmtWithResult(stmt))
    case ASTNOP => TypedNOP
    case x: ASTStatementWithResult => typeStmtWithResult(x)
  }

  def typeStmtWithResult(stmt: ASTStatementWithResult): TypedStatementWithResult = stmt match {
    case ASTAssignment(lhs, rhs) => {
      val typedRHS = typeExpression(rhs)
      updateValueEnvironment(lhs, typedRHS)
      updateEnvironment(lhs, extractType(typedRHS))
      TypedAssignment(typeIdentifier(lhs), typedRHS)
    }
    case exp: ASTExpression => typeExpression(exp)
  }

  def typeExpression(exp: ASTExpression): TypedExpression = exp match {
    case id: ASTIdentifier => typeIdentifier(id)
    case ASTInteger(value) => TypedInteger(value)
    case ASTFloatingPoint(value) => TypedFloatingPoint(value)
    case ASTString(value) => TypedString(value)
    case ASTUnaryExpression(exp, op) => {
      val typedExpression = typeExpression(exp)
      val operatorType = typeUnaryOperator(op)
      val unificationResult = resolvePolymorphicType(operatorType, FunctionType(extractType(typedExpression), newTV()))

      unificationResult match {
        case Some((FunctionType(_, resultType), _)) => TypedUnaryExpression(typedExpression, op, resultType)
        case _ => throw new TypeNotFoundError("Unary expression: " + ASTUnaryExpression(exp, op))
      }
    }
    case ASTBinaryExpression(a, op, b) => {
      val typedExpressionA = typeExpression(a)
      val typedExpressionB = typeExpression(b)
      val operatorType = typeBinaryOperator(op)
      val unificationResult = resolvePolymorphicType(operatorType, FunctionType(List(extractType(typedExpressionA),
        extractType(typedExpressionB)), newTV()))

      unificationResult match {
        case Some((FunctionType(_, resultType), _)) => {
          TypedBinaryExpression(typedExpressionA, op, typedExpressionB, resultType)
        }
        case _ => throw new TypeNotFoundError("Binary expression: " + ASTBinaryExpression(a, op, b))
      }
    }
    case ASTFunctionApplication(func, arguments) => {
      val typedFunc = typeIdentifier(func)
      val functionType = extractType(typedFunc)
      val typedArguments = arguments map { typeExpression(_) }

      val unificationResult = resolvePolymorphicType(functionType, FunctionType(typedArguments map
        { extractType(_) }, newTV()))

      unificationResult match {
        case Some((appliedFunType @ FunctionType(_, resultType), _)) =>
          TypedFunctionApplication(TypedIdentifier(typedFunc.value,appliedFunType), typedArguments,
                                   resolveValueReferences(resultType, typedArguments))
        case _ => throw new TypeNotFoundError("Function application could not be typed: " + exp)
      }
    }
    case ASTAnonymousFunction(parameters, body) => {
      val oldMappings = parameters map { case ASTIdentifier(id) => (id, typeEnvironment.get(id)) }
      parameters foreach { case ASTIdentifier(id) => updateEnvironment(id, newTV()) }

      val closure = (freeVariables(body) -- (parameters map { case ASTIdentifier(id) => id }).toSet).toList
      val typedBody = typeExpression(body)

      val typedParameters = parameters map {
        case ASTIdentifier(id) => TypedIdentifier(id, getType(id) match {
          case Some(t) => t
          case _ => throw new TypeNotFoundError("Type for parameter " + id + " could not be found")
        })
      }

      val functionType = FunctionType(typedParameters map { extractType(_) }, extractType(typedBody))

      oldMappings foreach {
        case (id, Some(datatype)) => updateEnvironment(id, datatype)
        case (_, None) => ;
      }

      TypedAnonymousFunction(typedParameters, typedBody, closure, functionType)
    }
    case ASTFunctionReference(function) => {
      val typedIdentifier = typeIdentifier(function)

      typedIdentifier.datatype match {
        case x: FunctionType => TypedFunctionReference(typedIdentifier, typedIdentifier.datatype)
        case _ => throw new TypingError("Identifier " + function.value + " has to be a function type")
      }
    }

    case _ => throw new NotImplementedError("")
  }

  def typeUnaryOperator(operator: UnaryOperator) = operator match {
    case TransposeOp | CellwiseTransposeOp => {
      val (t, a, b) = newTVV()
      PolymorphicType(List(FunctionType(MatrixType(t, a, b), MatrixType(t, b, a)),
        FunctionType(CharacterType, CharacterType),
        FunctionType(IntegerType, IntegerType),
        FunctionType(DoubleType, DoubleType)))
    }
    case PrePlusOp | PreMinusOp => {
      val (t, a, b) = newNTVV()
      PolymorphicType(List(FunctionType(MatrixType(t, a, b), MatrixType(t, a, b)),
        FunctionType(IntegerType, IntegerType),
        FunctionType(DoubleType, DoubleType)))
    }
  }

  def typeBinaryOperator(operator: BinaryOperator): Type = operator match {
    case ExpOp => {
      val a = newVV()
      val t = newNumericTV()
      PolymorphicType(List(FunctionType((MatrixType(t, a, a), DoubleType), MatrixType(t, a, a)),
        FunctionType((DoubleType, DoubleType), DoubleType)))
    }
    case CellwiseExpOp => {
      val (t, a, b) = newNTVV()
      PolymorphicType(List(FunctionType((MatrixType(t, a, b), DoubleType), MatrixType(t, a, b)),
        FunctionType((DoubleType, DoubleType), DoubleType)))
    }
    case PlusOp | MinusOp => {
      val (t, a, b) = newNTVV()
      val (t1, a1, b1) = newNTVV()
      val (t2, a2, b2) = newNTVV()
      PolymorphicType(List(FunctionType((MatrixType(t, a, b), MatrixType(t, a, b)), MatrixType(t, a, b)),
        FunctionType((MatrixType(t1, a1, b1), t1), MatrixType(t1, a1, b1)),
        FunctionType((t2, MatrixType(t2, a2, b2)), MatrixType(t2, a2, b2)),
        FunctionType((IntegerType, IntegerType), IntegerType),
        FunctionType((DoubleType, DoubleType), DoubleType)))
    }
    case MultOp => {
      val (t, a, b) = newNTVV()
      val c = newVV()
      val (t1, a1, b1) = newNTVV()
      val (t2, a2, b2) = newNTVV()
      PolymorphicType(List(FunctionType((MatrixType(t, a, b), MatrixType(t, b, c)), MatrixType(t, a, c)),
        FunctionType((MatrixType(t1, a1, b1), t1), MatrixType(t1, a1, b1)),
        FunctionType((t2, MatrixType(t2, a2, b2)), MatrixType(t2, a2, b2)),
        FunctionType((IntegerType, IntegerType), IntegerType),
        FunctionType((DoubleType, DoubleType), DoubleType)))
    }
    case DivOp => {
      val (t, a, b) = newNTVV()
      PolymorphicType(List(FunctionType((MatrixType(t, a, b), t), MatrixType(t, a, b)),
        FunctionType((DoubleType, DoubleType), DoubleType)))
    }
    case CellwiseMultOp | CellwiseDivOp => {
      val (t, a, b) = newNTVV()
      val (t1, a1, b1) = newNTVV()
      val (t2, a2, b2) = newNTVV()
      PolymorphicType(List(FunctionType((MatrixType(t, a, b), MatrixType(t, a, b)), MatrixType(t, a, b)),
        FunctionType((MatrixType(t1, a1, b1), t1), MatrixType(t1, a1, b1)),
        FunctionType((t2, MatrixType(t2, a2, b2)), MatrixType(t2, a2, b2)),
        FunctionType((IntegerType, IntegerType), IntegerType),
        FunctionType((DoubleType, DoubleType), DoubleType)))
    }
    case LogicalOrOp | LogicalAndOp => {
      PolymorphicType(List(FunctionType((IntegerType, IntegerType), IntegerType),
        FunctionType((DoubleType, DoubleType), DoubleType)))
    }
    case BinaryOrOp | BinaryAndOp => {
      val (t, a, b) = newNTVV()
      val (t1, a1, b1) = newNTVV()
      val (t2, a2, b2) = newNTVV()
      PolymorphicType(List(FunctionType((MatrixType(t, a, b), MatrixType(t, a, b)), MatrixType(IntegerType, a, b)),
        FunctionType((MatrixType(t1, a1, b1), t1), MatrixType(IntegerType, a1, b1)),
        FunctionType((t2, MatrixType(t2, a2, b2)), MatrixType(IntegerType, a2, b2)),
        FunctionType((IntegerType, IntegerType), IntegerType),
        FunctionType((DoubleType, DoubleType), IntegerType)))
    }
    case GTOp | GTEOp | LTOp | LTEOp | DEQOp => {
      val (t, a, b) = newNTVV()
      PolymorphicType(List(FunctionType((DoubleType, DoubleType), DoubleType),
        FunctionType((MatrixType(t, a, b), MatrixType(t, a, b)), MatrixType(IntegerType, a, b))))
    }
  }

  def typeIdentifier(id: ASTIdentifier) = id match {
    case ASTIdentifier(id) => {
      val idType = getType(id) match {
        case Some(t) => t
        case _ => throw new TypeNotFoundError("Identifier " + id + " is unbound")
      }
      TypedIdentifier(id, specializeType(idType))
    }
  }

  def typeFunction(func: ASTFunction) = {
    val typer = new Typer {}

    func.values foreach { typer.updateEnvironment(_, newTV()) }
    func.parameters foreach { typer.updateEnvironment(_, newTV()) }

    val typedBody = typer.typeProgram(func.body)
    val typedValues = func.values map { typer.typeIdentifier }
    val typedParameters = func.parameters map { typer.typeIdentifier }
    val resultType = if (typedValues.length == 0) VoidType else extractType(typedValues(0))
    val typedFunctionName = TypedIdentifier(func.identifier.value,
      generalizeType(FunctionType(typedParameters map { extractType }, resultType)))

    updateEnvironment(func.identifier, typedFunctionName.datatype)

    TypedFunction(typedValues, typedFunctionName, typedParameters, typedBody)
  }

}