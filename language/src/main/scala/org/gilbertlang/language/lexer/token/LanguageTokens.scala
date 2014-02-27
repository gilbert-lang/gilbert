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

import scala.util.parsing.combinator.token._

trait LanguageTokens extends Tokens {
  
  case class Identifier(identifier: String) extends Token {
    def chars = "identifier "+ identifier
  }
  
  case class Keyword(keyword: String) extends Token {
    def chars = "keyword " + keyword
  }
  
  case class StringLiteral(string: String) extends Token {
    def chars = "string " + string
  }
  
  case class IntegerLiteral(value: Int) extends Token {
    def chars = "integer " + value.toString
  }
  
  case class FloatingPointLiteral(value: Double) extends Token {
    def chars = "floating point " + value.toString
  }
  
  case class BooleanLiteral(value: Boolean) extends Token{
    def chars = "boolean " + value.toString
  }
  
  case class Comment(value: String) extends Token {
    def chars = "comment " + value
  }
  
  case class TypeAnnotation(value : String) extends Token {
    def chars = "type annotation " + value
  }
  
  case class Whitespace(whitespace: String) extends Token {
    def chars = "whitespace " + whitespace
  }
  
  case object Void extends Token {
    def chars = "void token"
  }
  
  def voidToken = Void
}