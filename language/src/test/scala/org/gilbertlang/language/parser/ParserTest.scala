package org.gilbertlang.language
package parser

import org.scalatest.Assertions
import org.junit.Test
import definition.Operators._
import org.gilbertlang.language.TestUtils

class ParserTest extends Parser with Assertions {

  import definition.AbstractSyntaxTree._
  import lexer.EOF

  @Test def testParser(){
    val expected = ASTProgram(List(ASTAssignment(ASTIdentifier("A"), ASTFunctionApplication(ASTIdentifier("load"),
      List(ASTString("inputfile"), ASTNumericLiteral(10), ASTNumericLiteral(10)))), ASTAssignment(ASTIdentifier("B"),
      ASTFunctionApplication(ASTIdentifier("bin"), List(ASTIdentifier("A")))), ASTAssignment(ASTIdentifier("C"),
      ASTBinaryExpression(ASTUnaryExpression(ASTIdentifier("B"), TransposeOp), MultOp, ASTIdentifier("B"))),
      ASTOutputResultStatement(ASTAssignment(ASTIdentifier("D"), ASTBinaryExpression(ASTIdentifier("C"),
        CellwiseDivOp, ASTFunctionApplication(ASTIdentifier("maxValue"), List(ASTIdentifier("C"))))))))
    val filename = "parserInput.gb"

    TestUtils.testParsingRessource(filename, expected)
  }

  @Test def testFunctionDefinitions() {
    val filename = "parserFunction.gb"
    val expected = ASTProgram(List(ASTFunction(List(ASTIdentifier("X")), ASTIdentifier("foobar"),
      List(ASTIdentifier("Y"), ASTIdentifier("Z")), ASTProgram(List(ASTAssignment(ASTIdentifier("A"),
        ASTBinaryExpression(ASTIdentifier("Y"), MultOp, ASTIdentifier("Z"))), ASTOutputResultStatement(
        ASTAssignment(ASTIdentifier("X"), ASTIdentifier("A"))))))))

    TestUtils.testParsingRessource(filename, expected)
  }

  @Test def testFunctionParameters() {
    val input = """(X,Y,Z)""";
    val expected = List(ASTIdentifier("X"), ASTIdentifier("Y"), ASTIdentifier("Z"))

    functionParams(input) match {
      case Success(result, in) =>
        expectResult(expected)(result)
      case _ => fail()
    }
  }

  @Test def testIdentifierList() {
    val input = """X,Y,Z""";
    val expected = List(ASTIdentifier("X"), ASTIdentifier("Y"), ASTIdentifier("Z"))

    identifierList(input) match {
      case Success(result, in) =>
        assert(in.first == EOF)
        expectResult(expected)(result)
      case _ => fail()
    }

  }

  @Test def testAnonymousFunction() {
    val filename = "parserAnonymousFunction.gb"
    val expected = ASTProgram(List(
      ASTOutputResultStatement(
        ASTAssignment(
          ASTIdentifier("X"),
          ASTAnonymousFunction(List(
            ASTIdentifier("A"),
            ASTIdentifier("B")),
            ASTBinaryExpression(
              ASTIdentifier("A"),
              PlusOp,
              ASTIdentifier("B")))))))
              
    TestUtils.testParsingRessource(filename, expected)
  }

  @Test def testFunctionReference() {
    val filename = "parserFunctionReference.gb"
    val expected = ASTProgram(List(
      ASTAssignment(
        ASTIdentifier("X"),
        ASTFunctionReference(
          ASTIdentifier("foobar")))))

    TestUtils.testParsingRessource(filename, expected)
  }

  @Test def testBooleanOperationParsing() {
    val expected = ASTProgram(List(
    ASTAssignment(ASTIdentifier("x"), ASTBinaryExpression(
    ASTBinaryExpression(ASTNumericLiteral(10.2), GTEOp, ASTNumericLiteral(3.2)),
    LogicalAndOp,
    ASTBoolean(value = true)
    ))
    ))
    val filename = "booleanOperationParsing.gb"
    TestUtils.testParsingRessource(filename, expected)
  }

  @Test def testCellArrayDefinitionParsing() {
    val expected = ASTProgram(List(
    ASTAssignment(
      ASTIdentifier("cell"),
      ASTCellArray(List(
        ASTBoolean(value = true),
        ASTBinaryExpression(
          ASTNumericLiteral(5),
          MultOp,
          ASTNumericLiteral(7)
        ),
        ASTFunctionApplication(
          ASTIdentifier("zeros"),
          List(
            ASTNumericLiteral(10),
            ASTNumericLiteral(10)
          )
        )
    ))
    )
    ))
    val filename = "testCellArrayDefinitionParsing.gb"

    TestUtils.testParsingRessource(filename, expected)
  }

  @Test def testCellArrayIndexingParsing() {
    val expected = ASTProgram(List(
      ASTCellArrayIndexing(
        ASTCellArrayIndexing(ASTIdentifier("a"),ASTNumericLiteral(0)),
        ASTNumericLiteral(1)
      )
    ))
    val filename = "testCellArrayIndexingParsing.gb"

    TestUtils.testParsingRessource(filename, expected)
  }


  @Test def testFunctionDefinitionCodeParsing() {
    val expected = ASTProgram(
      List(
        ASTAssignment(
          ASTIdentifier("x"),
          ASTNumericLiteral(1)
        ),
        ASTFunction(
          List(
            ASTIdentifier("y")
          ),
          ASTIdentifier("foo"),
          List(
            ASTIdentifier("z")
          ),
          ASTProgram(
            List(
              ASTAssignment(
                ASTIdentifier("y"),
                ASTBinaryExpression(
                  ASTIdentifier("z"),
                  PlusOp,
                  ASTNumericLiteral(1)
                )
              )
            )
          )
        ),
        ASTOutputResultStatement(
          ASTFunctionApplication(
            ASTIdentifier("foo"),
            List(
              ASTIdentifier("x")
            )
          )
        )
      )
    )

    val filename = "testFunctionDefinitionCodeParsing.gb"

    TestUtils.testParsingRessource(filename, expected)
  }
}
