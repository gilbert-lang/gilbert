package org.gilbertlang.language
package typer

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
    val expected = TypedProgram(
      List(
        TypedFunction(
          List(
            TypedIdentifier(
              "X",
              MatrixType(
                IntegerType,
                ValueVar(1),
                ValueVar(2)
              )
            )
          ),
          TypedIdentifier(
            "foobar",
            FunctionType(
              List(
                MatrixType(
                  IntegerType,
                  UniversalValue(
                    ValueVar(1)
                  ),
                  UniversalValue(
                    ValueVar(2)
                  )
                )
              ),
              MatrixType(
                IntegerType,
                UniversalValue(
                  ValueVar(1)
                ),
                UniversalValue(
                  ValueVar(2)
                )
              )
            )
          ),
          List(
            TypedIdentifier(
              "Y",
              MatrixType(
                IntegerType,
                ValueVar(1),
                ValueVar(2)
              )
            )
          ),
          TypedProgram(
            List(
              TypedOutputResultStatement(
                TypedAssignment(
                  TypedIdentifier(
                    "X",
                    MatrixType(
                      IntegerType,
                      ValueVar(1),
                      ValueVar(2)
                    )
                  ),
                  TypedBinaryExpression(
                    TypedIdentifier(
                      "Y",
                      TypeVar(3)
                    ),
                    PlusOp,
                    TypedInteger(1),
                    MatrixType(
                      IntegerType,
                      ValueVar(1),
                      ValueVar(2)
                    )
                  )
                )
              )
            )
          )
        )
      )
    )

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

  @Test def testCellArrayTyping {
    val filename = "testCellArrayTyping.gb"
    val expected = TypedProgram(List(TypedCellArray(List(TypedBoolean(true), TypedBinaryExpression(TypedInteger(2),
      PlusOp,TypedFloatingPoint(2.0),IntegerType), TypedFunctionApplication(TypedIdentifier("zeros",
      FunctionType(List(IntegerType, IntegerType),MatrixType(DoubleType,ReferenceValue(0),ReferenceValue(1)))),
      List(TypedInteger(10), TypedInteger(10)),MatrixType(DoubleType,IntValue(10),IntValue(10)))),
      ConcreteCellArrayType(List(BooleanType, IntegerType, MatrixType(DoubleType,IntValue(10),IntValue(10)))))))
    TestUtils.testTypingRessource(filename, expected)
  }

  @Test def testCellArrayIndexingTyping{
    val filename = "testCellArrayIndexingTyping.gb"
    val expected = TypedProgram(List(TypedAssignment(TypedIdentifier("x",ConcreteCellArrayType(List(BooleanType,
      DoubleType))),TypedCellArray(List(TypedBoolean(true), TypedFloatingPoint(2.0)),
      ConcreteCellArrayType(List(BooleanType,
      DoubleType)))), TypedCellArrayIndexing(TypedIdentifier("x",ConcreteCellArrayType(List(BooleanType, DoubleType))),
      TypedInteger(0),BooleanType)))

    TestUtils.testTypingRessource(filename, expected)
  }

  @Test def testAnonymousCellArrayFunctionTyping{
    val filename = "anonymousCellArrayFunctionTyping.gb"
    val expected = TypedProgram(
      List(
        TypedAnonymousFunction(
          List(
            TypedIdentifier(
              "x",
              InterimCellArrayType(
                List(
                   MatrixType(
                     NumericTypeVar(43),
                     ValueVar(65),
                     ValueVar(66)
                   ),
                  MatrixType(
                    NumericTypeVar(43),
                    ValueVar(65),
                    ValueVar(66)
                  )
                )
              )
            )
          ),
          TypedBinaryExpression(
            TypedCellArrayIndexing(
              TypedIdentifier(
                "x",
                TypeVar(0)
              ),
              TypedInteger(0),
              TypeVar(21)
            ),
            PlusOp,
            TypedCellArrayIndexing(
              TypedIdentifier(
                "x",
                InterimCellArrayType(
                  List(
                    MatrixType(
                      NumericTypeVar(43),
                      ValueVar(65),
                      ValueVar(66)
                    ),
                    MatrixType(
                      NumericTypeVar(43),
                      ValueVar(65),
                      ValueVar(66)
                    )
                  )
                )
              ),
              TypedInteger(1),
              TypeVar(22)
            ),
            MatrixType(
              NumericTypeVar(43),
              ValueVar(65),
              ValueVar(66)
            )
          ),
          List(),
          FunctionType(
            List(
              InterimCellArrayType(
                List(
                  MatrixType(
                    NumericTypeVar(43),
                    ValueVar(65),
                    ValueVar(66)
                  ),
                  MatrixType(
                    NumericTypeVar(43),
                    ValueVar(65),
                    ValueVar(66)
                  )
                )
              )
            ),
            MatrixType(
              NumericTypeVar(43),
              ValueVar(65),
              ValueVar(66)
            )
          )
        )
      )
    )

    TestUtils.testTypingRessource(filename, expected)
  }

}