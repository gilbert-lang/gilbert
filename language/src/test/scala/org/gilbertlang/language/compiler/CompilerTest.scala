package org.gilbertlang.language.compiler

import org.scalatest.Assertions
import org.junit.Test
import java.io.InputStreamReader
import scala.util.parsing.input.StreamReader
import org.gilbertlang.language.typer.Typer
import org.gilbertlang.language.parser.Parser
import java.io.IOException
import org.apache.commons.io.IOUtils
import org.gilbertlang.runtime.Executables._
import org.gilbertlang.language.format.VerboseTypedASTFormatter
import org.gilbertlang.runtime.shell.PlanPrinter
import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime.Operations.{LessEqualThan, Subtraction}

class CompilerTest extends Assertions {
  
  @Test def testFixpointCompilation(){
    var isReader:InputStreamReader = null;
    val expected = CompoundExecutable(List(WriteMatrix(randn(scalar(10.0),scalar(10.0),scalar(0.0), scalar(1.0))), 
        WriteMatrix(FixpointIteration(randn(scalar(10.0),scalar(10.0),scalar(0.0),scalar(1.0)), 
            function(1,MatrixParameter(0)), scalar(10.0), function(2, ScalarScalarTransformation(
          norm(
            CellwiseMatrixMatrixTransformation(
            MatrixParameter(0),
            MatrixParameter(1),
            Subtraction
            ),
            scalar(2.0)
          ),
          scalar(0.1),
          LessEqualThan
          ))))))
    try {
      isReader = new InputStreamReader(ClassLoader.getSystemResourceAsStream("compilerFixpoint.gb"))
      val reader = StreamReader(isReader)

      val typer = new Typer {}
      val parser = new Parser {}
      val compiler = new Compiler {}

      val ast = parser.parse(reader)

      ast match {
        case Some(parsedProgram) => {

          val typedAST = typer.typeProgram(parsedProgram)
          VerboseTypedASTFormatter.prettyPrint(typedAST)
          val compiledProgram = compiler.compile(typedAST)
          
          PlanPrinter.print(compiledProgram)
          
          expectResult(expected)(compiledProgram)
        }
        case _ => println("Could not parse program")
      }
    } catch{
      case exception: IOException => {
        sys.error(exception.getMessage())
        exception.printStackTrace()
      }
    } finally{
      IOUtils.closeQuietly(isReader)
    }
  }

  @Test def testBooleanOperationCompilation() {
    val expected = CompoundExecutable(List(
    WriteMatrix(ones(scalar(10.0), scalar(10.0))),
    WriteMatrix(zeros(scalar(10.0), scalar(10.0))),
    WriteScalar(scalar(0.1)),
    WriteScalar(ScalarScalarTransformation(
      norm(
        CellwiseMatrixMatrixTransformation(
          ones(scalar(10.0), scalar(10.0)),
          zeros(scalar(10.0), scalar(10.0)),
          Subtraction
        ),
        scalar(2.0)
      ),
      scalar(0.1),
      LessEqualThan
    ))

    ))
    val result = Gilbert.compileRessource("booleanOperationCompilation.gb")

    expectResult(expected)(result)
  }

}