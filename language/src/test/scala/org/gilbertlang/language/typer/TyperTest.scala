package org.gilbertlang.language
package typer

import org.scalatest.Assertions
import org.junit.Test
import definition.AbstractSyntaxTree._
import definition.TypedAst._
import definition.Types._
import definition.Values._
import parser.Parser
import scala.util.parsing.input.StreamReader
import java.io.InputStreamReader
import definition.Operators.{ DivOp, PlusOp }

class TyperTest extends Comparisons {

  @Test def testProgram {
    val ast = ASTProgram(List(ASTAssignment(ASTIdentifier("x"), ASTInteger(12))))
    val expected = TypedProgram(List(TypedAssignment(TypedIdentifier("x", IntegerType), TypedInteger(12))))
    val typer = new Typer {}
    val result = typer.typeProgram(ast)

    expectResult(expected)(result)
  }

  @Test def testCharacterIntegerUnification {
    val typer = new Typer {}

    expectResult(Some(IntegerType))(typer.unify(CharacterType, IntegerType))
  }

  @Test def testMatrixMatrixUnification1 {
    import definition.Values.Helper._
    import definition.Types.Helper._
    
    val typer = new Typer {}

    expectResult(Some(MatrixType(IntegerType, IntValue(10), IntValue(42))))(typer.unify(MatrixType(newTV(), newVV(), IntValue(42)),
      MatrixType(IntegerType, IntValue(10), newVV())))
  }

  @Test def testFunctionTyping {
    val typer = new Typer {}
    val parser = new Parser {}
    val expected = TypedProgram(List(
      TypedFunction(List(TypedIdentifier("X", MatrixType(IntegerType,
        (ValueVar(1)), (ValueVar(2))))),
        TypedIdentifier("foobar", FunctionType(List(
          MatrixType(IntegerType, UniversalValue(ValueVar(1)),
            UniversalValue(ValueVar(2)))), MatrixType(IntegerType,
          UniversalValue(ValueVar(1)), UniversalValue(ValueVar(2))))),
        List(TypedIdentifier("Y", MatrixType(IntegerType, (ValueVar(1)),
          (ValueVar(2))))), TypedProgram(List(
          TypedOutputResultStatement(TypedAssignment(TypedIdentifier("X", MatrixType(IntegerType,
            ValueVar(1), ValueVar(2))),
            TypedBinaryExpression(TypedIdentifier("Y",TypeVar(3)),
              PlusOp, TypedInteger(1), MatrixType(IntegerType, ValueVar(1),
                ValueVar(2))))))))))

    val fileName = "typerFunction.gb"
    val inputReader = StreamReader(new InputStreamReader(getClass().getClassLoader().getResourceAsStream(fileName)))
    parser.parse(inputReader) match {
      case Some(ast) =>
        val typedAST = typer.typeProgram(ast)
        checkTypeEquality(expected, typedAST)
      case _ => fail("Could not parse file " + fileName)
    }

  }

  @Test def testTypeWidening {
    val input = ASTBinaryExpression(ASTInteger(1), DivOp, ASTFloatingPoint(0.1))
    val expected = TypedBinaryExpression(TypedInteger(1), DivOp, TypedFloatingPoint(0.1),DoubleType)
    val typer = new Typer {}

    val result = typer.typeExpression(input)

    checkTypeEquality(expected,result)
  }

}