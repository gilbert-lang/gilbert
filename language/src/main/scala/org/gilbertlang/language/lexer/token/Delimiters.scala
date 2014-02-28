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

package org.gilbertlang.language.lexer.token

object Delimiters extends Enumeration {
  
  type Delimiters = Value
  
  val EQ = Value("=")
  val SEMICOLON = Value(";")  
  val COLON = Value(":")  
  val COMMA = Value(",")
  //TODO remove until necessary
  val GT = Value(">")
  //TODO remove until necessary
  val LT = Value("<")
  //TODO remove until necessary
  val GTE = Value(">=")
  //TODO remove until necessary
  val LTE = Value("<=")
  //TODO remove until necessary
  val DEQ = Value("==")
  val NEQ = Value("!=")
  val PLUS = Value("+")  
  val MINUS = Value("-")  
  val MULT = Value("*")  
  val CELLWISE_MULT = Value(".*")  
  val DIV = Value("/")  
  val CELLWISE_DIV = Value("./")  
  val TRANSPOSE = Value("\'")  
  val CELLWISE_TRANSPOSE = Value(".\'")
  val DQUOTE = Value("\"")  
  val LPAREN = Value("(")  
  val RPAREN = Value(")")  
  val LBRACKET = Value("[")  
  val RBRACKET = Value("]")  
  val LBRACE = Value("{")  
  val RBRACE = Value("}")  
  val NEWLINE = Value("\n")
  //TODO remove until necessary
  val SHORT_CIRCUIT_LOGICAL_OR = Value("||")
  //TODO remove until necessary
  val SHORT_CIRCUIT_LOGICAL_AND = Value("&&")
  //TODO remove until necessary
  val LOGICAL_OR = Value("|")
  //TODO remove until necessary
  val LOGICAL_AND = Value("&")
  val EXP = Value("^")  
  val CELLWISE_EXP = Value(".^")
  val AT = Value("@")
}