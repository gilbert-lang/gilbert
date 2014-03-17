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
import org.gilbertlang.language.definition.Types.IntegerType
import org.gilbertlang.language.definition.Types.DoubleType
import org.gilbertlang.language.definition.Types.BooleanType

object IntermediateAbstractSyntaxTree {

  def getType(statement: IntermediateStatement): Type = {
    statement match {
      case x: IntermediateExpression => x.datatype
      case IntermediateOutputResultStatement(innerStatement) => getType(innerStatement)
      case IntermediateAssignment(identifier, expression) => expression.datatype
      case IntermediateNOP => VoidType
    }
  }

  case class IntermediateProgram(statementsOrFunctions: List[IntermediateStatementOrFunction])

  sealed abstract class IntermediateStatementOrFunction

  case class IntermediateFunction(values: List[IntermediateIdentifier], identifier: IntermediateIdentifier,
                           parameters: List[IntermediateIdentifier], body: IntermediateProgram) extends
  IntermediateStatementOrFunction
  
  sealed abstract class IntermediateStatement extends IntermediateStatementOrFunction

  sealed abstract class IntermediateStatementWithResult extends IntermediateStatement

  case object IntermediateNOP extends IntermediateStatement
  case class IntermediateOutputResultStatement(statementWithResult: IntermediateStatementWithResult) extends IntermediateStatement
  case class IntermediateAssignment(identifier: IntermediateIdentifier, expression: IntermediateExpression) extends IntermediateStatementWithResult

  sealed abstract class IntermediateExpression extends IntermediateStatementWithResult {
    val datatype: Type
  }

  case class IntermediateString(value: String) extends IntermediateExpression {
    val datatype = StringType
  }
  
  case class IntermediateIdentifier(value: String, datatype: Type) extends IntermediateFunctionExpression
  
  case class IntermediateMatrix(value: List[IntermediateMatrixRow], datatype: MatrixType) extends IntermediateExpression
  
  case class IntermediateMatrixRow(value: List[IntermediateExpression])
  
  case class IntermediateUnaryExpression(expression: IntermediateExpression, expressionType: Type, operator: UnaryOperator,
                                  datatype: Type)
    extends IntermediateExpression
  
  case class IntermediateBinaryExpression(leftExpression: IntermediateExpression, leftType: Type, operator: BinaryOperator,
      rightExpression: IntermediateExpression, rightType: Type, datatype: Type)
    extends IntermediateExpression
  
  case class IntermediateFunctionApplication(function: IntermediateFunctionExpression, args: List[IntermediateExpression],
                                      argTypes: List[Type], datatype: Type)
    extends IntermediateExpression

  sealed trait IntermediateFunctionExpression extends IntermediateExpression
  case class IntermediateAnonymousFunction(parameters: List[IntermediateIdentifier], body: IntermediateExpression,
                                    closure: List[String], datatype: Type) extends IntermediateFunctionExpression
  case class IntermediateFunctionReference(reference: IntermediateIdentifier, datatype: Type) extends IntermediateFunctionExpression

  sealed trait IntermediateCellExpression extends IntermediateExpression
  case class IntermediateCellArray(elements: List[IntermediateExpression], datatype: Type) extends IntermediateCellExpression
  case class IntermediateCellArrayIndexing(cellArray: IntermediateExpression, index: Int,
                                    datatype: Type) extends IntermediateCellExpression

  sealed abstract class IntermediateScalar extends IntermediateExpression

  case class IntermediateInteger(value: Int) extends IntermediateScalar {
    val datatype = IntegerType
  }
  
  case class IntermediateFloatingPoint(value: Double) extends IntermediateScalar {
    val datatype = DoubleType
  }
  
  case class IntermediateBoolean(value: Boolean) extends IntermediateExpression{
    val datatype = BooleanType
  }

}