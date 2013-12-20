package org.gilbertlang.language
package format

import definition.AbstractSyntaxTree._
import definition.Operators.{TransposeOp, CellwiseTransposeOp, op2Str}

abstract class AbstractASTFormatter extends Formatter[ASTProgram] {
  protected val spacing: String
  private val nl = "\n";
  
  private def indentStr(indentation: Int) = {
    spacing * indentation
  }
	
  def prettyString(program: ASTProgram, indentation: Int): String = {
    program.statementsOrFunctions map { prettyString(_,indentation) } mkString(nl);
  }
  
  def prettyString(stmtOrFunction: ASTStatementOrFunction, indentation: Int): String = {
    stmtOrFunction match{
      case statement: ASTStatement => {
        prettyString(statement, indentation)
      }
      case function: ASTFunction => {
        prettyString(function,indentation)
      }
      case annotation: ASTTypeAnnotation => {
        indentStr(indentation) + "TypeAnnotation[" + annotation.annotation + "]"
      }
    }
  }
  
  def prettyString(function: ASTFunction, indentation: Int): String = {
    indentStr(indentation) + "Function[" + 
    "(" + (function.values map { prettyString(_,0) } mkString(",")) + ") " +
    prettyString(function.identifier,0) +
    "(" + (function.parameters map { prettyString(_, 0) } mkString(",")) + ")]" + nl +
    prettyString(function.body,indentation+1)
  }
  
  def prettyString(statement: ASTStatement, indentation: Int): String = {
    statement match {
      case ASTAssignment(lhs, rhs) => {
        indentStr(indentation) + "Assignment[" + prettyString(lhs,0) + "]" + nl +
        prettyString(rhs,indentation+1)
      }
      case x: ASTExpression => {
        prettyString(x,indentation)
      }
      case ASTOutputResultStatement(stmt) => {
        indentStr(indentation) + "Output" + nl +
        prettyString(stmt,indentation+1)
      }
      case ASTNOP => indentStr(indentation) + "NOP"
    }
  }
  
  def prettyString(expression: ASTExpression, indentation: Int): String ={
    expression match{
      case ASTString(value) => {
        indentStr(indentation) + "String(\"" + value + "\")"
      }
      case ASTInteger(value) => {
        indentStr(indentation) + "Integer(" + value + ")"
      }
      case ASTFloatingPoint(value) => {
        indentStr(indentation) + "FloatingPoint(" + value + ")"
      }
      case ASTIdentifier(id) => {
        indentStr(indentation) + "Identifier(" + id + ")"
      }
      case ASTFunctionReference(function) => {
        indentStr(indentation) + "FunctionReference(" + function + ")"
      }
      case ASTAnonymousFunction(arguments, body) => {
        indentStr(indentation) + "AnonymousFunction[(" +
        (arguments map { prettyString(_,0) } mkString(",")) + ")]" + nl +
        prettyString(body,indentation+1)
      }
      case ASTUnaryExpression(operand, TransposeOp | CellwiseTransposeOp) =>{
        indentStr(indentation) + prettyString(operand,0) + "'"
      }
      case ASTUnaryExpression(operand, operator) => {
        indentStr(indentation) + "UnaryExpression[" + op2Str(operator) + "]" + nl +
        prettyString(operand,indentation+1)
      }
      case ASTBinaryExpression(operandA, operator, operandB) => {
        indentStr(indentation) + "BinaryExpression["+ op2Str(operator) + "]" + nl +
        prettyString(operandA, indentation+1) + nl +
        prettyString(operandB, indentation+1)
      }
      case ASTFunctionApplication(function, arguments) => {
        indentStr(indentation) + "FunctionApplication[" + prettyString(function,0) + 
        "(" + ( arguments map { prettyString(_, 0) } mkString(",")) + ")"
      }
    }
  }
}