package org.gilbertlang.language
package typer

import org.junit.Test
import definition.AbstractSyntaxTree._
import definition.TypedAbstractSyntaxTree._
import definition.Types._
import definition.Values._
import org.gilbertlang.language.definition.Operators.{GTOp, MultOp, DivOp, PlusOp}

class TyperTest extends Comparisons {

  @Test def testProgram {
    val ast = ASTProgram(List(ASTAssignment(ASTIdentifier("x"), ASTInteger(12))))
    val expected = TypedProgram(List(TypedAssignment(TypedIdentifier("x", IntegerType), TypedInteger(12))))
    val typer = new Typer()
    val result = typer.typeProgram(ast)

    expectResult(expected)(result)
  }

  @Test def testCharacterIntegerUnification {
    val typer = new Typer()

    expectResult(Some(IntegerType))(typer.unify(CharacterType, IntegerType))
  }

  @Test def testMatrixMatrixUnification1 {
    import definition.Values.Helper._
    import definition.Types.Helper._
    
    val typer = new Typer()

    expectResult(Some(MatrixType(IntegerType, IntValue(10), IntValue(42))))(typer.unify(MatrixType(newTV(), newVV(), IntValue(42)),
      MatrixType(IntegerType, IntValue(10), newVV())))
  }

  @Test def testFunctionTyping {
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
                      MatrixType(
                        IntegerType,
                        ValueVar(1),
                        ValueVar(2)
                      )
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

    val filename = "typerFunction.gb"
    TestUtils.testTypingRessource(filename, expected)
  }

  @Test def testTypeWideningNumeric {
    val input = ASTBinaryExpression(ASTInteger(1), DivOp, ASTFloatingPoint(0.1))
    val expected = TypedBinaryExpression(TypedInteger(1), DivOp, TypedFloatingPoint(0.1),DoubleType)
    val typer = new Typer()

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
                      IntegerType,
                      IntegerType
                    ),
                    MatrixType(
                      DoubleType,
                      ReferenceValue(0),
                      ReferenceValue(1)
                    )
                  )
                ),
                List(
                  TypedInteger(10),
                  TypedInteger(10)
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
        ),
        TypedAssignment(
          TypedIdentifier(
            "b",
            MatrixType(
              IntegerType,
              IntValue(1),
              IntValue(1)
            )
          ),
          TypedFunctionApplication(
            TypedIdentifier(
              "f",
              FunctionType(
                List(
                  MatrixType(
                    IntegerType,
                    IntValue(1),
                    IntValue(1)
                  )
                ),
                MatrixType(
                  IntegerType,
                  IntValue(1),
                  IntValue(1)
                )
              )
            ),
            List(
              TypedFunctionApplication(
                TypedIdentifier(
                  "ones",
                  FunctionType(
                    List(
                      IntegerType,
                      IntegerType
                    ),
                    MatrixType(
                      IntegerType,
                      ReferenceValue(0),
                      ReferenceValue(1)
                    )
                  )
                ),
                List(
                  TypedInteger(1),
                  TypedInteger(1)
                ),
                MatrixType(
                  IntegerType,
                  IntValue(1),
                  IntValue(1)
                )
              )
            ),
            MatrixType(
              IntegerType,
              IntValue(1),
              IntValue(1)
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
              IntegerType,
              IntValue(1),
              IntValue(1)
            )
          ),
          TypedFunctionApplication(
            TypedIdentifier(
              "ones",
              FunctionType(
                List(
                  IntegerType,
                  IntegerType
                ),
                MatrixType(
                  IntegerType,
                  ReferenceValue(0),
                  ReferenceValue(1)
                )
              )
            ),
            List(
              TypedInteger(1),
              TypedInteger(1)
            ),
            MatrixType(
              IntegerType,
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
                IntegerType,
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
                  IntegerType,
                  UniversalValue(
                    ValueVar(67)
                  ),
                  UniversalValue(
                    ValueVar(68)
                  )
                )
              ),
              MatrixType(
                IntegerType,
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
                IntegerType,
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
                    IntegerType,
                    ValueVar(67),
                    ValueVar(68)
                  )
                ),
                TypedBinaryExpression(
                  TypedIdentifier(
                    "z",
                    MatrixType(
                      IntegerType,
                      ValueVar(67),
                      ValueVar(68)
                    )
                  ),
                  PlusOp,
                  TypedInteger(1),
                  MatrixType(
                    IntegerType,
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
                    IntegerType,
                    IntValue(1),
                    IntValue(1)
                  )
                ),
                MatrixType(
                  IntegerType,
                  IntValue(1),
                  IntValue(1)
                )
              )
            ),
            List(
              TypedIdentifier(
                "x",
                MatrixType(
                  IntegerType,
                  IntValue(1),
                  IntValue(1)
                )
              )
            ),
            MatrixType(
              IntegerType,
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
            TypedInteger(1),
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
            TypedInteger(1),
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
            TypedInteger(1),
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
                IntegerType,
                IntValue(2),
                IntValue(2)
              )
            ),
            TypedFunctionApplication(
              TypedIdentifier(
                "ones",
                FunctionType(
                  List(
                    IntegerType,
                    IntegerType
                  ),
                  MatrixType(
                    IntegerType,
                    ReferenceValue(0),
                    ReferenceValue(1)
                  )
                )
              ),
              List(
                TypedInteger(2),
                TypedInteger(2)
              ),
              MatrixType(
                IntegerType,
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
                  IntegerType,
                  IntValue(2),
                  IntValue(2)
                )
              ),
              GTOp,
              TypedInteger(0),
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
                IntegerType,
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