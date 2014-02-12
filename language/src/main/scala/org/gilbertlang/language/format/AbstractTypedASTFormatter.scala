package org.gilbertlang.language
package format

import definition.Types._
import definition.TypedAst._
import definition.Operators.{TransposeOp, op2Str, CellwiseTransposeOp}

abstract class AbstractTypedASTFormatter extends Formatter[TypedProgram] {
  private val typeFormatter = new TypeFormatter
  protected val spacing: String
  protected val nl = "\n"
  protected val verbose: Boolean

  private def indentStr(indentation: Int): String = {
    spacing * indentation
  }

  private def str(str: String, indentation: Int = 0): String = {
    indentStr(indentation) + str
  }

  private def strnl(str: String, indentation: Int = 0): String = {
    indentStr(indentation) + str + "\n"
  }

  private def verboseType(datatype: Type): String = {
    if (verbose) str(": ") + typeFormatter.prettyString(datatype)
    else ""
  }

  private def nonVerboseType(datatype: Type): String = {
    if (!verbose) str(": ") + typeFormatter.prettyString(datatype)
    else ""
  }

  def prettyPrint(expression: TypedExpression) {
    println(prettyString(expression, 0))
  }
  
  def prettyString(program: TypedProgram) = {
    prettyString(program, 0)
  }

  def prettyString(program: TypedProgram, indentation: Int): String = {
    val stmtsOrFuncs = program.statementsOrFunctions map { prettyString(_, indentation + 1) } mkString ("\n")
    strnl("Program(", indentation) + stmtsOrFuncs + nl + strnl(")", indentation)
  }

  def prettyString(stmtOrFunction: TypedStatementOrFunction, indentation: Int): String = {
    stmtOrFunction match {
      case x: TypedStatement => prettyString(x, indentation)
      case x: TypedFunction => prettyString(x, indentation)
    }
  }

  def prettyString(function: TypedFunction, indentation: Int): String = {
    str("Function [", indentation) + (function.values map { prettyString(_, 0) } mkString (", ")) + str("] = ") +
      function.identifier + str("(") + (function.parameters map { prettyString(_, 0) } mkString (", ")) + str("): ") +
      typeFormatter.prettyString(function.identifier.datatype) + str("{") + nl +
      prettyString(function.body, indentation + 1) +
      str("}", indentation)
  }

  def prettyString(stmtWithResult: TypedStatementWithResult, indentation: Int): String = {
    stmtWithResult match {
      case x: TypedExpression => prettyString(x, indentation) + nonVerboseType(x.datatype)
      case TypedAssignment(rhs, lhs) => {
        prettyString(rhs, indentation) + str(" = ") + prettyString(lhs, 0) + nonVerboseType(lhs.datatype)
      }
    }
  }

  def prettyString(expression: TypedExpression, indentation: Int): String = {
    (expression match {
      case x: TypedIdentifier => prettyString(x, indentation)
      case TypedInteger(value) => str(value.toString, indentation)
      case TypedFloatingPoint(value) => str(value.toString, indentation)
      case TypedString(value) => str(value, indentation)
      case TypedUnaryExpression(exp, TransposeOp, _) => str("(", indentation) + prettyString(exp, 0) + str(")") +
        op2Str(TransposeOp)
      case TypedUnaryExpression(exp, CellwiseTransposeOp, _) => str("(", indentation) + prettyString(exp, 0) +
        str(")") + op2Str(TransposeOp)
      case TypedUnaryExpression(exp, op, _) => str("(", indentation) + str(op2Str(op)) + prettyString(exp, 0) + str(")")
      case TypedBinaryExpression(a, op, b, _) => str("(", indentation) + prettyString(a, 0) + str(" ") + op2Str(op) +
        str(" ") + prettyString(b, 0) + str(")")
      case TypedFunctionApplication(func, args, _) => {
        prettyString(func, indentation) + str("(") + (args map { prettyString(_, 0) } mkString (", ")) + str(")")
      }
      case TypedFunctionReference(func, _) => str("@", indentation) + prettyString(func, 0)
      case TypedAnonymousFunction(params, body, closure, _) => str("@(", indentation) +
        (params map { prettyString(_, 0) } mkString (", ")) + str(")") + str("(") + (closure mkString (", ")) +
        str(")") + prettyString(body, 0)
    }) + verboseType(expression.datatype)
  }

  def prettyString(identifier: TypedIdentifier, indentation: Int): String = {
    str(identifier.value, indentation)
  }

  def prettyString(statement: TypedStatement, indentation: Int): String = {
    statement match {
      case TypedOutputResultStatement(stmt: TypedStatementWithResult) => {
        prettyString(stmt, indentation)
      }
      case TypedNOP => strnl("NOP", indentation)
      case x: TypedStatementWithResult => prettyString(x, indentation)
    }
  }

}