package org.gilbertlang.language
package typer

import org.scalatest.Assertions
import org.junit.Test
import definition.AbstractSyntaxTree._
import definition.Operators._
import definition.Types._
import org.gilbertlang.language.definition.TypedAbstractSyntaxTree._

class LogicalOperationsTest extends Assertions {

  @Test def testGreaterThan {
    val ast = ASTProgram(List(
        ASTBinaryExpression(ASTNumericLiteral(2.5), GTOp,
            ASTNumericLiteral(3.1))))
    val expected = TypedProgram(List(
        TypedBinaryExpression(TypedNumericLiteral(2.5), GTOp,
            TypedNumericLiteral(3.1), BooleanType)
        ))
    val typer = new Typer()
    val result = typer.typeProgram(ast)
    
    expectResult(expected)(result)
  }
  
  @Test def testLogicalAnd {
    val ast = ASTProgram(List(
        ASTBinaryExpression(
            ASTBinaryExpression(ASTNumericLiteral(4.2),DEQOp,ASTNumericLiteral(4.2))
            ,LogicalAndOp,
            ASTBinaryExpression(ASTNumericLiteral(1.3),LTEOp,ASTNumericLiteral(0.8)))))
            
    val expected = TypedProgram(List(
        TypedBinaryExpression(
            TypedBinaryExpression(
                TypedNumericLiteral(4.2),
                DEQOp,
                TypedNumericLiteral(4.2),
                BooleanType),
            LogicalAndOp,
            TypedBinaryExpression(
                TypedNumericLiteral(1.3),
                LTEOp,
                TypedNumericLiteral(0.8),
                BooleanType),
            BooleanType)))
    
    val typer = new Typer{}
    val result = typer.typeProgram(ast)
    
    expectResult(expected)(result)
  }
  
}