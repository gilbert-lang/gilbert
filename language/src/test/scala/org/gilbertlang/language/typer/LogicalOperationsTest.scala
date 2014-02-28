package org.gilbertlang.language
package typer

import org.scalatest.Assertions
import org.junit.Test
import definition.AbstractSyntaxTree._
import definition.Operators._
import definition.TypedAst._
import definition.Types._

class LogicalOperationsTest extends Assertions {

  @Test def testGreaterThan {
    val ast = ASTProgram(List(
        ASTBinaryExpression(ASTFloatingPoint(2.5), GTOp,
            ASTFloatingPoint(3.1))))
    val expected = TypedProgram(List(
        TypedBinaryExpression(TypedFloatingPoint(2.5), GTOp,
            TypedFloatingPoint(3.1), BooleanType)
        ))
    val typer = new Typer {}
    val result = typer.typeProgram(ast)
    
    expectResult(expected)(result)
  }
  
  @Test def testLogicalAnd {
    val ast = ASTProgram(List(
        ASTBinaryExpression(
            ASTBinaryExpression(ASTFloatingPoint(4.2),DEQOp,ASTFloatingPoint(4.2))
            ,LogicalAndOp,
            ASTBinaryExpression(ASTFloatingPoint(1.3),LTEOp,ASTFloatingPoint(0.8)))))
            
    val expected = TypedProgram(List(
        TypedBinaryExpression(
            TypedBinaryExpression(
                TypedFloatingPoint(4.2),
                DEQOp,
                TypedFloatingPoint(4.2),
                BooleanType),
            LogicalAndOp,
            TypedBinaryExpression(
                TypedFloatingPoint(1.3),
                LTEOp,
                TypedFloatingPoint(0.8),
                BooleanType),
            BooleanType)))
    
    val typer = new Typer{}
    val result = typer.typeProgram(ast)
    
    expectResult(expected)(result)
  }
  
}