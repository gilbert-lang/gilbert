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

  def letter:Parser[Char] = elem("letter", _.isLetter)

  def letter(chr: Char): Parser[Char] = elem(chr.toString, _ == chr)

  def digit:Parser[Char] = elem("digit", _.isDigit)

  def whitespace:Parser[Char] = elem("whitespace", ch => ch <= ' ' && ch != EofCh && ch != '\n')

  def chrExcept(except: List[Char]):Parser[Char] = {
    elem("Except: " + except.mkString(""), chr => except forall ( _ != chr))
  }

  implicit def tilde2Str[A <: { def mkString(delim: String):String }, B <: { def mkString(delim: String):String }]
  (x: ~[A, B]):{ def mkString(delim:String):String } = 
  { new { def mkString(delim: String):String = x._1.mkString("") + x._2.mkString("") }}

  def token(previousToken: Token): Parser[Token] =  (
    letter ~ rep(letter | digit | '_') ^^ { case h ~ t => processIdentifier(h + (t.mkString("")))}
      | digit ~ rep(digit) ~ opt('.' ~ rep(digit)) ~ (letter('e') | letter('E')) ~ opt(letter('+') | letter('-')) ~ digit ~ rep(digit) ^^ {
      case h ~ t ~ p ~ l ~ s ~ e ~ r =>
        val a = (h::t).mkString("")
        val b = p match {
          case Some(p ~ l) => p + l.mkString("")
          case None => ""
        }
        val c = s match {
          case Some(pm) => pm
          case None => ""
        }

        val d = (e::r).mkString("")

        FloatingPointLiteral((a+b+l+c+d).toDouble)
    }
      | digit ~ rep(digit) ~ '.' ~ rep(digit) ^^ { case h~t~p~r => FloatingPointLiteral((h + t.mkString("") + p  + r
      .mkString("")).toDouble) }
      | '.' ~ digit ~ rep(digit) ^^ { case p ~ h ~ r => FloatingPointLiteral(("0" + p + h + r.mkString("")).toDouble) }
      | digit ~ rep(digit) ^^ { 
        case h ~ t => 
          IntegerLiteral((h::t).mkString("").toInt) 
        }
      | whitespace ~ rep(whitespace) ^^ { case h ~ t => Whitespace(h + t.mkString(""))}
      | guard(Parser { in => if (isTransposable(previousToken)) Failure("failure",in) else Success("success",
      in) })  ~> '\'' ~> rep(chrExcept(List('\'', '\n', EofCh))) <~ '\'' ^^ { case l => StringLiteral(l.mkString("")) }
      | '\"' ~> rep(chrExcept(List('\"', '\n', EofCh))) <~ '\"' ^^ { case l => StringLiteral(l.mkString("")) }
      | '%' ~> '>' ~> rep(chrExcept(List('\n',EofCh))) ^^ { case l => TypeAnnotation(l.mkString("")) }
      | '%'~> rep(chrExcept(List('\n', EofCh))) ^^ { case l => Comment(l.mkString("")) }
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
        case Keywords.TRUE => BooleanLiteral(true)
        case Keywords.FALSE => BooleanLiteral(false)
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