package org.gilbertlang.language

import java.io.Reader
import java.io.InputStreamReader
import java.io.FileReader
import org.apache.commons.io.IOUtils
import org.gilbertlang.optimizer.Optimizer
import org.gilbertlang.runtime.Executables.Executable
import org.gilbertlang.language.parser.Parser
import org.gilbertlang.language.typer.Typer
import org.gilbertlang.language.compiler.Compiler
import scala.util.parsing.input.StreamReader
import org.gilbertlang.language.parser.ParseError

object Gilbert {
  def compile(inputFile: String): Executable = {
    var inputReader: Reader = null
    
    try{
      inputReader = new FileReader(inputFile)
      compile(inputReader)
    } finally{
      IOUtils.closeQuietly(inputReader)
    }
  }
  
  def compileRessource(ressourceString: String): Executable = {
    var inputReader: Reader = null
    
    try{
      inputReader = new InputStreamReader(ClassLoader.getSystemResourceAsStream(ressourceString))
      
      compile(inputReader)
    }finally{
      IOUtils.closeQuietly(inputReader)
    }
  }
  
  def compile(inputReader: Reader): Executable = {
    val reader = StreamReader(inputReader)
    val parser = new Parser{}
    val typer = new Typer()
    val compiler = new Compiler{}

    val ast = parser.parse(reader) match {
      case None => throw new ParseError("Could not parse input")
      case Some(astValue) => astValue
    }

    val typedAST = typer.typeProgram(ast)

    compiler.compile(typedAST)
  }

  def optimize(program: Executable, mmReorder: Boolean, transposePushdown: Boolean): Executable = {
    val afterTransposePushdown = transposePushdown match {
      case true => Optimizer.transposePushdown(program)
      case false => program
    }

    val afterMMReordering = mmReorder match {
      case true => Optimizer.mmReorder(afterTransposePushdown)
      case false => afterTransposePushdown
    }

    afterMMReordering
  }
}