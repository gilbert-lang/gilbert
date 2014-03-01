package org.gilbertlang.language

import java.io.InputStreamReader
import org.apache.commons.io.IOUtils
import org.gilbertlang.language.definition.AbstractSyntaxTree.ASTProgram
import org.gilbertlang.language.parser.Parser
import scala.util.parsing.input.StreamReader
import org.scalatest.Assertions
import scala.collection.mutable.ListBuffer
import org.gilbertlang.language.definition.TypedAst.TypedProgram
import org.gilbertlang.language.typer.Typer

/**
 * Created by till on 28/02/14.
 */
object TestUtils extends Parser with Assertions
{
  def testParsingRessource(filename: String, expected: ASTProgram) {
    val input = ClassLoader.getSystemResourceAsStream(filename)
    var inputReader: InputStreamReader = null

    try{
      inputReader = new InputStreamReader(input)
      val reader = StreamReader(inputReader)

      phrase(program)(reader) match {
        case Success(actual, next) =>
          assert(next.atEnd, "Input has not been read completely.")
          expectResult(expected)(actual)
        case _ => fail("Could not parse file " + filename)
      }
    }finally{
      IOUtils.closeQuietly(inputReader)
    }


  }

  def testTypingRessource(filename: String, expected: TypedProgram){
    val input = ClassLoader.getSystemResourceAsStream(filename)
    var inputReader: InputStreamReader = null

    try{
      inputReader = new InputStreamReader(input)
      val reader = StreamReader(inputReader)

      phrase(program)(reader) match {
        case Success(actual, next) =>
          assert(next.atEnd, "Input has not been read completely.")
          val typer = new Typer{}

          val typedAST = typer.typeProgram(actual)

          expectResult(expected)(typedAST)
      }

    }
  }

  def getTokensFromRessource(filename: String) = {
    val input = ClassLoader.getSystemResourceAsStream(filename)
    var inputReader: InputStreamReader = null

    try{
      inputReader = new InputStreamReader(input)
      val reader = StreamReader(inputReader)

      var tokenReader = lexer(reader)
      val result = new ListBuffer[lexer.Token]()
      while(!tokenReader.atEnd){
        result += tokenReader.first
        tokenReader = tokenReader.rest
      }

      result.toList
    }
  }


}
