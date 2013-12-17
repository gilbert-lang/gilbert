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

package org.gilbertlang.examples

import org.gilbertlang.language.parser.Parser
import org.gilbertlang.language.typer.Typer
import org.gilbertlang.language.compiler.Compiler
import scala.util.parsing.input.StreamReader
import java.io.InputStreamReader

object PlayWithLanguage {

  def main(args: Array[String]): Unit = {

    //TODO close stream
    val reader = StreamReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream("play.gb")))

    val typer = new Typer {}
    val compiler = new Compiler {}
    val parser = new Parser {}

    val ast = parser.parse(reader)

    ast match {
      case Some(parsedProgram) => {

        val typedAST = typer.typeProgram(parsedProgram)
        val compiledProgram = compiler.compile(typedAST)

        println(compiledProgram)
      }
      case _ => println("Could not parse program")
    }

  }
  
}
