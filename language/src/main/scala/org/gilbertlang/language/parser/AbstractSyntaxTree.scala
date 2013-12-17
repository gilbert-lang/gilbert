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

package org.gilbertlang.language.parser

import org.gilbertlang.language.definition.Operators.{BinaryOperator, UnaryOperator}

object AbstractSyntaxTree {
  
  case class ASTProgram(statementsOrFunctions: List[ASTStatementOrFunction])

  sealed abstract class ASTStatementOrFunction

  case class ASTFunction(values: List[ASTIdentifier], identifier: ASTIdentifier, parameters: List[ASTIdentifier],
                         body: ASTProgram) extends ASTStatementOrFunction

  case class ASTTypeAnnotation(annotation: String) extends ASTStatementOrFunction

  sealed abstract class ASTStatement  extends ASTStatementOrFunction
  sealed abstract class ASTStatementWithResult extends ASTStatement
  case class ASTOutputResultStatement(statementWithResult: ASTStatementWithResult) extends ASTStatement
  case object ASTNOP extends ASTStatement
  case class ASTAssignment(identifier: ASTIdentifier, expression: ASTExpression) extends ASTStatementWithResult

  sealed abstract class ASTExpression extends ASTStatementWithResult

  case class ASTString(value: String) extends ASTExpression
  case class ASTIdentifier(value: String) extends ASTExpression
  case class ASTMatrix(value: List[ASTMatrixRow]) extends ASTExpression
  case class ASTMatrixRow(value: List[ASTExpression]) extends ASTExpression
  case class ASTUnaryExpression(expression:ASTExpression, operator: UnaryOperator) extends ASTExpression
  case class ASTBinaryExpression(leftExpression: ASTExpression, operator: BinaryOperator, 
                                 rightExpression: ASTExpression) extends ASTExpression
  case class ASTFunctionApplication(function: ASTIdentifier, args: List[ASTExpression]) extends ASTExpression
  case class ASTAnonymousFunction(parameters: List[ASTIdentifier], body: ASTExpression) extends ASTExpression
  case class ASTFunctionReference(reference: ASTIdentifier) extends ASTExpression

  sealed abstract class ASTScalar extends ASTExpression
  case class ASTInteger(value: Int) extends ASTScalar
  case class ASTFloatingPoint(value: Double) extends ASTScalar
}
