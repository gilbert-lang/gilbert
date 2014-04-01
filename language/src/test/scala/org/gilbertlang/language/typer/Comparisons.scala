package org.gilbertlang.language
package typer

import definition.Types._
import definition.Values._
import org.scalatest.Assertions
import org.gilbertlang.language.definition.TypedAbstractSyntaxTree._

trait Comparisons extends Assertions {
  val universalTypeMapping = scala.collection.mutable.HashMap[AbstractTypeVar, AbstractTypeVar]()
  val typeVarMapping = scala.collection.mutable.HashMap[AbstractTypeVar, AbstractTypeVar]()
  val universalValueMapping = scala.collection.mutable.HashMap[ValueVar, ValueVar]()
  val valueVarMapping = scala.collection.mutable.HashMap[ValueVar, ValueVar]()

  private def clear() {
    universalTypeMapping.clear()
    typeVarMapping.clear()
    universalValueMapping.clear()
    valueVarMapping.clear()
  }

  def checkTypeEquality(expected: TypedProgram, actual: TypedProgram) {
    clear()
    checkProgram(expected, actual)
  }
  
  def checkTypeEquality(expected: TypedExpression, actual: TypedExpression) {
    clear()
    checkExpression(expected, actual)
  }

  def checkProgram(expected: TypedProgram, actual: TypedProgram) {
    (expected, actual) match {
      case (TypedProgram(exp), TypedProgram(act)) =>
        expectResult(exp.length)(act.length)
        exp.zip(act).foreach({ case (a, b) => checkStmtOrFuncEquality(a, b) })
      case _ => fail("expected and actual are not of type TypedProgram")
    }
  }

  def checkStmtOrFuncEquality(expected: TypedStatementOrFunction, actual: TypedStatementOrFunction) {
    (expected, actual) match {
      case (e: TypedStatement, a: TypedStatement) => checkStatement(e, a)
      case (e: TypedFunction, a: TypedFunction) => checkFunction(e, a)
      case _ => fail("expected and actual don't have the same TypedStatementOrFunction type")
    }
  }

  def checkStatement(expected: TypedStatement, actual: TypedStatement) {
    (expected, actual) match {
      case (TypedNOP, TypedNOP) =>
      case (TypedAssignment(expLhs, expRhs), TypedAssignment(actLhs, actRhs)) =>
        checkIdentifier(expLhs, actLhs)
        checkExpression(expRhs, actRhs)
      case (TypedOutputResultStatement(expStmt), TypedOutputResultStatement(actStmt)) =>
        checkStatement(expStmt, actStmt)
      case (expExpression: TypedExpression, actExpression: TypedExpression) =>
        checkExpression(expExpression, actExpression)
      case _ => fail("expected and actual do not have the same statement type")
    }
  }

  def checkFunction(expected: TypedFunction, actual: TypedFunction) {
    (expected, actual) match {
      case (TypedFunction(expResult, expId, expParams, expBody),
        TypedFunction(actResult, actId, actParams, actBody)) =>
        expectResult(expResult.length)(actResult.length)
        expResult zip actResult foreach { case (e, a) => checkIdentifier(e, a) }
        checkIdentifier(expId, actId)
        expectResult(expParams.length)(actParams.length)
        expParams zip actParams foreach { case (e, a) => checkIdentifier(e, a) }
        checkProgram(expBody, actBody)
    }
  }

  def checkExpression(expected: TypedExpression, actual: TypedExpression) {
    (expected, actual) match {
      case (e: TypedIdentifier, a: TypedIdentifier) => checkIdentifier(e, a)
      case (TypedMatrix(expRows, expMType), TypedMatrix(actRows, actMType)) =>
        checkType(expMType, actMType)
        expectResult(expRows.length)(actRows.length)
        expRows zip actRows foreach {
          case (TypedMatrixRow(expEntries), TypedMatrixRow(actEntries)) =>
            expectResult(expEntries.length)(actEntries.length)
            expEntries zip actEntries foreach { case (e, a) => checkExpression(e, a) }
        }
      case (TypedUnaryExpression(exp, expOp, expType), TypedUnaryExpression(act, actOp, actType)) =>
        checkExpression(exp, act)
        expectResult(expOp)(actOp)
        checkType(expType, actType)
      case (TypedBinaryExpression(expA, expOp, expB, expType),
        TypedBinaryExpression(actA, actOp, actB, actType)) =>
        checkExpression(expA, actA)
        checkExpression(expB, actB)
        expectResult(expOp)(actOp)
        checkType(expType, actType)
      case (TypedFunctionApplication(expFunc, expParams, expType),
        TypedFunctionApplication(actFunc, actParams, actType)) =>
        checkExpression(expFunc, actFunc)
        expectResult(expParams.length)(actParams.length)
        expParams zip actParams foreach { case (e, a) => checkExpression(e, a) }
        checkType(expType, actType)
      case (TypedAnonymousFunction(expParams, expBody, expClosure, expType),
          TypedAnonymousFunction(actParams, actBody, actClosure, actType)) =>
        expectResult(expParams.length)(actParams.length)
        expParams zip actParams foreach { case(e,a) => checkIdentifier(e,a)}
        expectResult(expClosure.length)(actClosure.length)
        expClosure zip actClosure foreach { case (e,a) => expectResult(e)(a)}
        checkExpression(expBody, actBody)
        checkType(expType, actType)
      case (TypedFunctionReference(exp, expType), TypedFunctionReference(act, actType)) =>
        checkIdentifier(exp,act)
        checkType(expType, actType)

      case (TypedCellArray(elementsA, typeA), TypedCellArray(elementsB, typeB)) =>
        elementsA zip elementsB foreach { case (a,b) => checkExpression(a,b) }
        checkType(typeA, typeB)
      case (TypedCellArrayIndexing(cellArrayA, indexA, typeA), TypedCellArrayIndexing(cellArrayB, indexB, typeB)) =>
        checkExpression(cellArrayA, cellArrayB)
        assert(indexA == indexB)
        checkType(typeA, typeB)
      case _ => expectResult(expected)(actual)
    }
  }

  def checkIdentifier(expected: TypedIdentifier, actual: TypedIdentifier) {
    checkType(expected.datatype, actual.datatype)
    expectResult(expected.value)(actual.value)
  }
  
  def checkType(expected: Type, actual: Type) {
    (expected, actual) match {
      case (UniversalType(exp), UniversalType(act)) =>
        val resolvedExp = universalTypeMapping.getOrElseUpdate(exp, act)
        expectResult(resolvedExp)(act)
      case (e: AbstractTypeVar, a: AbstractTypeVar) =>
        val resolvedExp = typeVarMapping.getOrElseUpdate(e,a)
        expectResult(resolvedExp)(a)
      case (FunctionType(expArgs, expResult), FunctionType(actArgs, actResult)) =>
      case (PolymorphicType(exp), PolymorphicType(act)) =>
        expectResult(exp.length)(act.length)
        exp zip act foreach { case(e,a) => checkType(e,a)}
      case (MatrixType(expEType, expRows, expCols), MatrixType(actEType, actRows, actCols)) =>
        checkType(expEType,actEType)
        checkValue(expRows,actRows)
        checkValue(expCols, actCols)
      case (a: CellArrayType, b: CellArrayType) =>
        expectResult(a.types.length)(b.types.length)
        a.types zip b.types foreach { case (a,b) => checkType(a,b)}
      case _ => expectResult(expected)(actual)
    }
  }
  
  def checkValue(expected: Value, actual: Value) {
    (expected, actual) match{
      case (UniversalValue(exp), UniversalValue(act)) =>
        val resolvedExp = universalValueMapping.getOrElseUpdate(exp,act)
        expectResult(resolvedExp)(act)
      case (exp: ValueVar, act: ValueVar) =>
        val resolvedExp = valueVarMapping.getOrElseUpdate(exp,act)
        expectResult(resolvedExp)(act)
      case _ => expectResult(expected)(actual)
    }
  }
}