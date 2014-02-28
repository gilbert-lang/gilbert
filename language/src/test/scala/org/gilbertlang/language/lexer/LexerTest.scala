package org.gilbertlang.language.lexer

import org.scalatest.Assertions
import token.DiscardWhitespaces
import java.io.{InputStreamReader, InputStream, FileReader}
import scala.util.parsing.input.StreamReader
import token.LanguageTokens
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.junit.Test
import org.scalatest.Spec
import scala.util.parsing.input.CharSequenceReader
import scala.util.parsing.input.CharArrayReader
import org.apache.commons.io.IOUtils

class LexerTest extends Lexer with DiscardWhitespaces with Assertions {

  @Test def testMLexer() {
    val expected = List(Identifier("A"), Keyword("="), Identifier("load"), Keyword("("),
      StringLiteral("path to file"), Keyword(","), IntegerLiteral(10), Keyword(","),
      IntegerLiteral(10), Keyword(")"), Keyword("\n"), Identifier("B"), Keyword("="), Identifier("bin"),
      Keyword("("), Identifier("A"), Keyword(")"), Keyword("\n"), Identifier("C"), Keyword("="),
      Identifier("B"), Keyword("\'"), Keyword("*"), Identifier("B"), Keyword("\n"), Identifier("D"),
      Keyword("="), Identifier("C"), Keyword("./"), Identifier("maxValue"), Keyword("("), Identifier("C"),
      Keyword(")"), Keyword("\n"), Keyword("\n"), EOF)
    val lexer = new Lexer with DiscardWhitespaces

    val inputFileURL = ClassLoader.getSystemResource("lexerInput.gb");
    var fileReader: FileReader = null

    try {
      fileReader = new FileReader(inputFileURL.toURI.getPath())

      val streamReader = StreamReader(fileReader)

      val result = lex(streamReader)

      expectResult(expected)(result)
    } finally {
      IOUtils.closeQuietly(fileReader)
    }
  }

  @Test def testEOFChar() {
    val expected = List(Identifier("X"), EOF)
    val input = "X";

    val result = lex(input)

    expectResult(expected)(result)
  }

  @Test def testZeroLexing() {
    val expected = List(Identifier("A"), Keyword("="), Identifier("rand"), Keyword("("), IntegerLiteral(0),
 Keyword(")"), EOF)

    val inputFileURL = ClassLoader.getSystemResource("zeroLexing.gb");
    var fileReader: FileReader = null
    
    try{
      fileReader = new FileReader(inputFileURL.toURI.getPath())
      val streamReader = StreamReader(fileReader)
      val result = lex(streamReader)
      
      expectResult(expected)(result)
    }finally{
      IOUtils.closeQuietly(fileReader)
    }
  }

  @Test def testBooleanLexing() {
    val expected = List(BooleanLiteral(true), Keyword("&"), BooleanLiteral(false), EOF)
    val input = ClassLoader.getSystemResourceAsStream("booleanLexing.gb")
    var inputReader: InputStreamReader = null
    try{
      inputReader = new InputStreamReader(input)
      val streamReader = StreamReader(inputReader)
      val result = lex(streamReader)

      expectResult(expected)(result)
    }finally{
      IOUtils.closeQuietly(inputReader)
    }
  }
}