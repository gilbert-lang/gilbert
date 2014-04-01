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

package org.gilbertlang.language.lexer


import scala.util.parsing.input.CharArrayReader.EofCh
import org.gilbertlang.language.lexer.token.{Delimiters, Keywords, LanguageTokens}
import scala.collection.immutable.HashSet
import scala.language.reflectiveCalls
import scala.language.implicitConversions

trait Lexer extends Scanners with LanguageTokens {

  val keywords = HashSet[String]((for (value <- Keywords.values) yield value.toString).toSeq:_*)

  def accept(token: Token) = { true }

  def character:Parser[Char] = elem("letter", _.isLetter)

  def character(chr: Char): Parser[Char] = elem(chr.toString, _ == chr)

  def digit:Parser[Char] = elem("digit", _.isDigit)

  def whitespace:Parser[Char] = elem("whitespace", ch => ch <= ' ' && ch != EofCh && ch != '\n')

  def chrExcept(except: List[Char]):Parser[Char] = {
    elem("Except: " + except.mkString(""), chr => except forall ( _ != chr))
  }

  implicit def removeTildeInParserCombinatorPairs[A <: { def mkString(delim: String):String }, B <: { def mkString(delim: String):String }]
  (x: ~[A, B]):{ def mkString(delim:String):String } = 
  { new { def mkString(delim: String):String = x._1.mkString("") + x._2.mkString("") }}

  def token(previousToken: Token): Parser[Token] =  (
    character ~ rep(character | digit | '_') ^^ { case firstLetter ~ lettersOrDigits => processIdentifier(firstLetter + (lettersOrDigits.mkString("")))}
      | rep1(digit) ~ opt('.' ~ rep(digit)) ~ (character('e') | character('E')) ~ opt(character('+') | character('-')) ~
      rep1(digit) ^^ {
      case digits ~ pointAndDecimalFraction ~ exponentCharacter ~ sign ~ exponent =>
        val a = (digits).mkString("")
        val b = pointAndDecimalFraction match {
          case Some(p ~ l) => p + l.mkString("")
          case None => ""
        }
        val c = sign match {
          case Some(pm) => pm
          case None => ""
        }

        val d = (exponent).mkString("")

        NumericLiteral((a+b+exponentCharacter+c+d).toDouble)
    }
      | rep1(digit) ~ '.' ~ rep(digit) ^^ { case digits~point~fraction => NumericLiteral((digits.mkString("") + point
      + fraction.mkString("")).toDouble) }
      | '.' ~ rep1(digit) ^^ { case point ~ fraction => NumericLiteral(("0" + point + fraction.mkString(""))
      .toDouble) }
      | rep1(digit) ^^ { digits => NumericLiteral((digits).mkString("").toDouble)}
      | rep1(whitespace) ^^ { whitespaces => Whitespace(whitespaces.mkString(""))}
      | guard(Parser {
          in =>
            if (isTransposable(previousToken)) Failure("failure",in)
            else Success("success",in)
        })  ~> '\'' ~> rep(chrExcept(List('\'', '\n', EofCh))) <~ '\'' ^^ { case characters => StringLiteral(characters.mkString("")) }
      | '\"' ~> rep(chrExcept(List('\"', '\n', EofCh))) <~ '\"' ^^ { case characters => StringLiteral(characters.mkString("")) }
      | '%' ~> '>' ~> rep(chrExcept(List('\n',EofCh))) ^^ { case characters => TypeAnnotation(characters.mkString("")) }
      | '%'~> rep(chrExcept(List('\n', EofCh))) ^^ { case characters => Comment(characters.mkString("")) }
      | EofCh ^^^ EOF
      | delimiterParser
      | failure("illegal character"))

  private def isTransposable(token: Token):Boolean = {
    token match {
      case Identifier(_) | Keyword(")") | Keyword("]") | Keyword("}") => true
      case _ => false
    }
  }

  private def processIdentifier(identifier: String):Token = {
    if (keywords contains identifier) {
      val keyword = Keywords.withName(identifier)
      keyword match {
        case Keywords.TRUE => BooleanLiteral(value = true)
        case Keywords.FALSE => BooleanLiteral(value = false)
        case _ => Keyword(identifier)
      }
    } else {
      Identifier(identifier)
    }
  }

  private lazy val delimiters = {
    val delimiterValues = for (value <- Delimiters.values) yield value.toString
    val sortedDelimiterValues = delimiterValues.toList.sortWith(_.length >= _.length)

    (sortedDelimiterValues map ( s => accept(s.toList) ^^^ Keyword(s) ))
      .foldRight(failure("No such delimiter found"): Parser[Token])((delimiter,right) => delimiter | right)
  }

  private def delimiterParser : Parser[Token] = delimiters
}