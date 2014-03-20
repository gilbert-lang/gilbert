package org.gilbertlang.language
package typer

import org.junit.Test
import definition.AbstractSyntaxTree._
import definition.Types._
import definition.Values._
import org.gilbertlang.language.definition.Operators.{GTOp, MultOp, DivOp, PlusOp}
import org.gilbertlang.language.definition.TypedAbstractSyntaxTree._

class TyperTest extends Comparisons {

  @Test def testProgram {
    val ast = ASTProgram(List(ASTAssignment(ASTIdentifier("x"), ASTNumericLiteral(12))))
    val expected = TypedProgram(List(TypedAssignment(TypedIdentifier("x", DoubleType), TypedNumericLiteral(12))))
    val typer = new Typer()
    val result = typer.typeProgram(ast)

    expectResult(expected)(result)
  }

  @Test def testCharacterIntegerUnification {
    val typer = new Typer()

    expectResult(Some(DoubleType))(typer.unify(CharacterType, DoubleType))
  }

  @Test def testMatrixMatrixUnification1 {
    import definition.Values.Helper._
    import definition.Types.Helper._
    
    val typer = new Typer()

    expectResult(Some(MatrixType(DoubleType, IntValue(10), IntValue(42))))(typer.unify(MatrixType(newTV(), newVV(), IntValue(42)),
      MatrixType(DoubleType, IntValue(10), newVV())))
  }

  @Test def testFunctionTyping {
    val expected = TypedProgram(
      List(
        TypedFunction(
          List(
            TypedIdentifier(
              "X",
              MatrixType(
                DoubleType,
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
                  DoubleType,
                  UniversalValue(
                    ValueVar(1)
                  ),
                  UniversalValue(
                    ValueVar(2)
                  )
                )
              ),
              MatrixType(
                DoubleType,
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
                DoubleType,
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
                      DoubleType,
                      ValueVar(1),
                      ValueVar(2)
                    )
                  ),
                  TypedBinaryExpression(
                    TypedIdentifier(
                      "Y",
                      MatrixType(
                        DoubleType,
                        ValueVar(1),
                        ValueVar(2)
                      )
                    ),
                    PlusOp,
                    TypedNumericLiteral(1),
                    MatrixType(
                      DoubleType,
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

    val filename = "typerFunction.gb"
    TestUtils.testTypingRessource(filename, expected)
  }

  @Test def testTypeWideningNumeric {
    val input = ASTBinaryExpression(ASTNumericLiteral(1), DivOp, ASTNumericLiteral(0.1))
    val expected = TypedBinaryExpression(TypedNumericLiteral(1), DivOp, TypedNumericLiteral(0.1),DoubleType)
    val typer = new Typer()

    val result = typer.typeExpression(input)

    checkTypeEquality(expected,result)
  }

  @Test def testCellArrayTyping {
    val filename = "testCellArrayTyping.gb"
    val expected = TypedProgram(List(TypedCellArray(List(TypedBoolean(true), TypedBinaryExpression(TypedNumericLiteral(2),
      PlusOp,TypedNumericLiteral(2.0),DoubleType), TypedFunctionApplication(TypedIdentifier("zeros",
      FunctionType(List(DoubleType, DoubleType),MatrixType(DoubleType,ReferenceValue(0),ReferenceValue(1)))),
      List(TypedNumericLiteral(10), TypedNumericLiteral(10)),MatrixType(DoubleType,IntValue(10),IntValue(10)))),
      ConcreteCellArrayType(List(BooleanType, DoubleType, MatrixType(DoubleType,IntValue(10),IntValue(10)))))))
    TestUtils.testTypingRessource(filename, expected)
  }

  @Test def testCellArrayIndexingTyping{
    val filename = "testCellArrayIndexingTyping.gb"
    val expected = TypedProgram(List(TypedAssignment(TypedIdentifier("x",ConcreteCellArrayType(List(BooleanType,
      DoubleType))),TypedCellArray(List(TypedBoolean(true), TypedNumericLiteral(2.0)),
      ConcreteCellArrayType(List(BooleanType,
      DoubleType)))), TypedCellArrayIndexing(TypedIdentifier("x",ConcreteCellArrayType(List(BooleanType,
      DoubleType))),0,BooleanType)))

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
              0,
              MatrixType(
                NumericTypeVar(43),
                ValueVar(65),
                ValueVar(66)
              )
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
              1,
              MatrixType(
                NumericTypeVar(43),
                ValueVar(65),
                ValueVar(66)
              )
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

  @Test def testGeneralization {
    val filename = "testGeneralization.gb"
    val expected = TypedProgram(
      List(
        TypedAssignment(
          TypedIdentifier(
            "f",
            FunctionType(
              List(
                TypeVar(21)
              ),
              TypeVar(21)
            )
          ),
          TypedAnonymousFunction(
            List(
              TypedIdentifier(
                "x",
                TypeVar(0)
              )
            ),
            TypedIdentifier(
              "x",
              TypeVar(0)
            ),
            List(),
            FunctionType(
              List(
                TypeVar(0)
              ),
              TypeVar(0)
            )
          )
        ),
        TypedOutputResultStatement(
          TypedAssignment(
            TypedIdentifier(
              "a",
              MatrixType(
                DoubleType,
                IntValue(10),
                IntValue(10)
              )
            ),
            TypedFunctionApplication(
              TypedIdentifier(
                "f",
                FunctionType(
                  List(
                    MatrixType(
                      DoubleType,
                      IntValue(10),
                      IntValue(10)
                    )
                  ),
                  MatrixType(
                    DoubleType,
                    IntValue(10),
                    IntValue(10)
                  )
                )
              ),
              List(
                TypedFunctionApplication(
                  TypedIdentifier(
                    "zeros",
                    FunctionType(
                      List(
                        DoubleType,
                        DoubleType
                      ),
                      MatrixType(
                        DoubleType,
                        ReferenceValue(0),
                        ReferenceValue(1)
                      )
                    )
                  ),
                  List(
                    TypedNumericLiteral(10),
                    TypedNumericLiteral(10)
                  ),
                  MatrixType(
                    DoubleType,
                    IntValue(10),
                    IntValue(10)
                  )
                )
              ),
              MatrixType(
                DoubleType,
                IntValue(10),
                IntValue(10)
              )
            )
          )
        ),
        TypedOutputResultStatement(
          TypedAssignment(
            TypedIdentifier(
              "b",
              BooleanType
            ),
            TypedFunctionApplication(
              TypedIdentifier(
                "f",
                FunctionType(
                  List(
                    BooleanType
                  ),
                  BooleanType
                )
              ),
              List(
                TypedBoolean(
                  true
                )
              ),
              BooleanType
            )
          )
        )
      )
    )

    TestUtils.testTypingRessource(filename, expected)
  }

  @Test def testFunctionDefinitionCodeTyping{
    val filename = "testFunctionDefinitionCodeTyping.gb"
    val expected = TypedProgram(
      List(
        TypedAssignment(
          TypedIdentifier(
            "x",
            MatrixType(
              DoubleType,
              IntValue(1),
              IntValue(1)
            )
          ),
          TypedFunctionApplication(
            TypedIdentifier(
              "ones",
              FunctionType(
                List(
                  DoubleType,
                  DoubleType
                ),
                MatrixType(
                  DoubleType,
                  ReferenceValue(0),
                  ReferenceValue(1)
                )
              )
            ),
            List(
              TypedNumericLiteral(1),
              TypedNumericLiteral(1)
            ),
            MatrixType(
              DoubleType,
              IntValue(1),
              IntValue(1)
            )
          )
        ),
        TypedFunction(
          List(
            TypedIdentifier(
              "y",
              MatrixType(
                DoubleType,
                ValueVar(67),
                ValueVar(68)
              )
            )
          ),
          TypedIdentifier(
            "foo",
            FunctionType(
              List(
                MatrixType(
                  DoubleType,
                  UniversalValue(
                    ValueVar(67)
                  ),
                  UniversalValue(
                    ValueVar(68)
                  )
                )
              ),
              MatrixType(
                DoubleType,
                UniversalValue(
                  ValueVar(67)
                ),
                UniversalValue(
                  ValueVar(68)
                )
              )
            )
          ),
          List(
            TypedIdentifier(
              "z",
              MatrixType(
                DoubleType,
                ValueVar(67),
                ValueVar(68)
              )
            )
          ),
          TypedProgram(
            List(
              TypedAssignment(
                TypedIdentifier(
                  "y",
                  MatrixType(
                    DoubleType,
                    ValueVar(67),
                    ValueVar(68)
                  )
                ),
                TypedBinaryExpression(
                  TypedIdentifier(
                    "z",
                    MatrixType(
                      DoubleType,
                      ValueVar(67),
                      ValueVar(68)
                    )
                  ),
                  PlusOp,
                  TypedNumericLiteral(1),
                  MatrixType(
                    DoubleType,
                    ValueVar(67),
                    ValueVar(68)
                  )
                )
              )
            )
          )
        ),
        TypedOutputResultStatement(
          TypedFunctionApplication(
            TypedIdentifier(
              "foo",
              FunctionType(
                List(
                  MatrixType(
                    DoubleType,
                    IntValue(1),
                    IntValue(1)
                  )
                ),
                MatrixType(
                  DoubleType,
                  IntValue(1),
                  IntValue(1)
                )
              )
            ),
            List(
              TypedIdentifier(
                "x",
                MatrixType(
                  DoubleType,
                  IntValue(1),
                  IntValue(1)
                )
              )
            ),
            MatrixType(
              DoubleType,
              IntValue(1),
              IntValue(1)
            )
          )
        )
      )
    )

    TestUtils.testTypingRessource(filename, expected)
  }

  @Test def testTypeWidening{
    val filename = "testTypeWidening.gb"
    val expected = TypedProgram(
      List(
        TypedOutputResultStatement(
          TypedBinaryExpression(
            TypedNumericLiteral(1),
            PlusOp,
            TypeConversion(
              TypedBoolean(true),
              DoubleType
            ),
            DoubleType
          )
        ),
        TypedOutputResultStatement(
          TypedBinaryExpression(
            TypedNumericLiteral(1),
            PlusOp,
            TypeConversion(
              TypedBoolean(false),
              DoubleType
            ),
            DoubleType
          )
        ),
        TypedAssignment(
          TypedIdentifier(
            "x",
            BooleanType
          ),
          TypedBoolean(false)
        ),
        TypedOutputResultStatement(
          TypedBinaryExpression(
            TypedNumericLiteral(1),
            PlusOp,
            TypeConversion(
              TypedIdentifier(
                "x",
                BooleanType
              ),
              DoubleType
            ),
            DoubleType
          )
        ),
        TypedOutputResultStatement(
          TypedAssignment(
            TypedIdentifier(
              "y",
              MatrixType(
                DoubleType,
                IntValue(2),
                IntValue(2)
              )
            ),
            TypedFunctionApplication(
              TypedIdentifier(
                "ones",
                FunctionType(
                  List(
                    DoubleType,
                    DoubleType
                  ),
                  MatrixType(
                    DoubleType,
                    ReferenceValue(0),
                    ReferenceValue(1)
                  )
                )
              ),
              List(
                TypedNumericLiteral(2),
                TypedNumericLiteral(2)
              ),
              MatrixType(
                DoubleType,
                IntValue(2),
                IntValue(2)
              )
            )
          )
        ),
        TypedOutputResultStatement(
          TypedAssignment(
            TypedIdentifier(
              "c",
              MatrixType(
                BooleanType,
                IntValue(2),
                IntValue(2)
              )
            ),
            TypedBinaryExpression(
              TypedIdentifier(
                "y",
                MatrixType(
                  DoubleType,
                  IntValue(2),
                  IntValue(2)
                )
              ),
              GTOp,
              TypedNumericLiteral(0),
              MatrixType(
                BooleanType,
                IntValue(2),
                IntValue(2)
              )
            )
          )
        ),
        TypedOutputResultStatement(
          TypedBinaryExpression(
            TypedIdentifier(
              "y",
              MatrixType(
                DoubleType,
                IntValue(2),
                IntValue(2)
              )
            ),
            MultOp,
            TypeConversion(
              TypedIdentifier(
                "c",
                MatrixType(
                  BooleanType,
                  IntValue(2),
                  IntValue(2)
                )
              ),
              MatrixType(
                DoubleType,
                IntValue(2),
                IntValue(2)
              )
            ),
            MatrixType(
              DoubleType,
              IntValue(2),
              IntValue(2)
            )
          )
        )
      )
    )

    TestUtils.testTypingRessource(filename, expected)
  }

}