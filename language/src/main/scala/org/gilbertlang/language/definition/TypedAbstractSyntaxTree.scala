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

package org.gilbertlang.language.definition

import org.gilbertlang.language.definition.Types.Type
import org.gilbertlang.language.definition.Types.VoidType
import org.gilbertlang.language.definition.Types.StringType
import org.gilbertlang.language.definition.Types.MatrixType
import org.gilbertlang.language.definition.Operators.UnaryOperator
import org.gilbertlang.language.definition.Operators.BinaryOperator
import org.gilbertlang.language.definition.Types.DoubleType
import org.gilbertlang.language.definition.Types.BooleanType

object TypedAbstractSyntaxTree {

  def getType(statement: TypedStatement): Type = {
    statement match {
      case x: TypedExpression => x.datatype
      case TypedOutputResultStatement(innerStatement) => getType(innerStatement)
      case TypedAssignment(identifier, expression) => expression.datatype
      case TypedNOP => VoidType
    }
  }

  case class TypedProgram(statementsOrFunctions: List[TypedStatementOrFunction])

  sealed abstract class TypedStatementOrFunction

  case class TypedFunction(values: List[TypedIdentifier], identifier: TypedIdentifier,
                           parameters: List[TypedIdentifier], body: TypedProgram) extends
  TypedStatementOrFunction
  
  sealed abstract class TypedStatement extends TypedStatementOrFunction

  sealed abstract class TypedStatementWithResult extends TypedStatement

  case object TypedNOP extends TypedStatement
  case class TypedOutputResultStatement(statementWithResult: TypedStatementWithResult) extends TypedStatement
  case class TypedAssignment(identifier: TypedIdentifier, expression: TypedExpression) extends TypedStatementWithResult

  sealed abstract class TypedExpression extends TypedStatementWithResult {
    val datatype: Type
  }

  case class TypedString(value: String) extends TypedExpression {
    val datatype = StringType
  }
  
  case class TypedIdentifier(value: String, datatype: Type) extends TypedFunctionExpression
  
  case class TypedMatrix(value: List[TypedMatrixRow], datatype: MatrixType) extends TypedExpression
  
  case class TypedMatrixRow(value: List[TypedExpression])
  
  case class TypedUnaryExpression(expression: TypedExpression, operator: UnaryOperator,
                                  datatype: Type)
    extends TypedExpression
  
  case class TypedBinaryExpression(leftExpression: TypedExpression, operator: BinaryOperator,
      rightExpression: TypedExpression, datatype: Type)
    extends TypedExpression
  
  case class TypedFunctionApplication(function: TypedFunctionExpression, args: List[TypedExpression], datatype: Type)
    extends TypedExpression

  sealed trait TypedFunctionExpression extends TypedExpression
  case class TypedAnonymousFunction(parameters: List[TypedIdentifier], body: TypedExpression,
                                    closure: List[String], datatype: Type) extends TypedFunctionExpression
  case class TypedFunctionReference(reference: TypedIdentifier, datatype: Type) extends TypedFunctionExpression

  sealed trait TypedCellExpression extends TypedExpression
  case class TypedCellArray(elements: List[TypedExpression], datatype: Type) extends TypedCellExpression
  case class TypedCellArrayIndexing(cellArray: TypedExpression, index: Int,
                                    datatype: Type) extends TypedCellExpression

  sealed abstract class TypedScalar extends TypedExpression

  case class TypedNumericLiteral(value: Double) extends TypedScalar {
    val datatype = DoubleType
  }
  
  case class TypedBoolean(value: Boolean) extends TypedExpression{
    val datatype = BooleanType
  }

  case class TypeConversion(expression: TypedExpression, targetType: Type) extends TypedExpression{
    val datatype = targetType
  }

}