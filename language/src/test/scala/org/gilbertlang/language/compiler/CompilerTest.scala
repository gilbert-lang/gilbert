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
import org.gilbertlang.runtime.Operations._
import org.gilbertlang.runtime.RuntimeTypes._
import org.gilbertlang.runtime.Executables.MatrixMult
import org.gilbertlang.runtime.Executables.WriteCellArray
import org.gilbertlang.runtime.Executables.boolean
import org.gilbertlang.runtime.Executables.CellwiseMatrixMatrixTransformation
import org.gilbertlang.runtime.Executables.scalar
import org.gilbertlang.runtime.Executables.ScalarScalarTransformation
import org.gilbertlang.runtime.Executables.ScalarMatrixTransformation
import org.gilbertlang.runtime.Executables.CellArrayExecutable
import org.gilbertlang.runtime.Executables.WriteMatrix
import org.gilbertlang.runtime.Executables.Transpose
import org.gilbertlang.runtime.Executables.MatrixParameter
import org.gilbertlang.runtime.Executables.CellArrayParameter
import scala.Some
import org.gilbertlang.runtime.Executables.WriteScalar
import org.gilbertlang.runtime.Executables.function
import org.gilbertlang.runtime.Executables.zeros
import org.gilbertlang.runtime.Executables.eye
import org.gilbertlang.runtime.Executables.FixpointIteration
import org.gilbertlang.runtime.Executables.CellArrayReferenceMatrix
import org.gilbertlang.runtime.Executables.ones
import org.gilbertlang.runtime.Executables.randn
import org.gilbertlang.runtime.Executables.WriteFunction
import org.gilbertlang.runtime.Executables.norm
import org.gilbertlang.runtime.Executables.CompoundExecutable
import org.gilbertlang.runtime.Executables.eye
import org.gilbertlang.runtime.Executables.MatrixMult
import org.gilbertlang.runtime.Executables.WriteCellArray
import org.gilbertlang.runtime.Executables.boolean
import org.gilbertlang.runtime.Executables.CellwiseMatrixMatrixTransformation
import org.gilbertlang.runtime.Executables.scalar
import org.gilbertlang.runtime.Executables.ScalarScalarTransformation
import org.gilbertlang.runtime.Executables.CellArrayExecutable
import org.gilbertlang.runtime.Executables.FixpointIteration
import org.gilbertlang.runtime.Executables.WriteMatrix
import org.gilbertlang.runtime.Executables.CellArrayReferenceMatrix
import org.gilbertlang.runtime.Executables.Transpose
import org.gilbertlang.runtime.Executables.ones
import org.gilbertlang.runtime.Executables.MatrixParameter
import org.gilbertlang.runtime.Executables.randn
import org.gilbertlang.runtime.Executables.WriteFunction
import scala.Some
import org.gilbertlang.runtime.Executables.norm
import org.gilbertlang.runtime.RuntimeTypes.MatrixType
import org.gilbertlang.runtime.Executables.CompoundExecutable
import org.gilbertlang.runtime.Executables.WriteScalar
import org.gilbertlang.runtime.Executables.function
import org.gilbertlang.runtime.Executables.zeros

class CompilerTest extends Assertions {
  
  @Test def testFixpointCompilation(){
    val filename = "compilerFixpoint.gb"
    val expected = CompoundExecutable(
      List(
        WriteMatrix(
          randn(
            scalar(10.0),
            scalar(10.0),
            scalar(0.0),
            scalar(1.0)
          )
        ),
        WriteMatrix(
          FixpointIteration(
            randn(
              scalar(10.0),
              scalar(10.0),
              scalar(0.0),
              scalar(1.0)
            ),
            function(
              1,
              MatrixParameter(0)
            ),
            scalar(10.0),
            function(
              2,
              ScalarScalarTransformation(
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
              )
            )
          )
        )
      )
    )

    val result = Gilbert.compileRessource(filename)
    expectResult(expected)(result)
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

  @Test def testCellArrayCompilation(){
    val expected = CompoundExecutable(List(WriteCellArray(CellArrayExecutable(List(boolean(value = true),
      zeros(scalar(10.0),scalar(10.0))))), WriteMatrix(zeros(scalar(10.0),scalar(10.0)))))
    val filename = "testCellArrayCompilation.gb"

    val result = Gilbert.compileRessource(filename)

    expectResult(expected)(result)
  }

  @Test def testCellArrayAnonymousFunctionCompilation(){

    val expected = CompoundExecutable(
        List(
          WriteFunction(VoidExecutable),
          WriteMatrix(
             CellwiseMatrixMatrixTransformation(
               CellArrayReferenceMatrix(
                 CellArrayExecutable(
                   List(
                     zeros(
                       scalar(1.0),
                       scalar(1.0)
                     ),
                     ones(
                       scalar(1.0),
                       scalar(1.0)
                     )
                   )
                 ),
                 0,
                 MatrixType(
                  DoubleType,
                  Some(1),
                  Some(1)
                 )
               ),
               CellArrayReferenceMatrix(
                 CellArrayExecutable(
                   List(
                     zeros(
                       scalar(1.0),
                       scalar(1.0)
                     ),
                     ones(
                       scalar(1.0),
                       scalar(1.0)
                     )
                   )
                 ),
                 1,
                 MatrixType(
                  DoubleType,
                  Some(1),
                  Some(1)
                 )
               ),
               Addition
             )
          )
        )
    )

    val filename ="testCellArrayAnonymousFunctionCompilation.gb"

    val result = Gilbert.compileRessource(filename)

    expectResult(expected)(result)

  }

  @Test def testFunctionDefinitionCodeCompiling(){
    val filename = "testFunctionDefinitionCodeCompiling.gb"
    val expected = CompoundExecutable(
      List(
        WriteScalar(
          scalar(1.0)
        ),
        WriteMatrix(
          ones(
            scalar(2.0),
            scalar(2.0)
          )
        )
      )
    )

    val result = Gilbert.compileRessource(filename)

    expectResult(expected)(result)
  }

  @Test def testNNMFCompilation() {
    val filename = "testNNMF.gb"
    val expected =
      CompoundExecutable(
        List(
          WriteCellArray(
            CellArrayExecutable(
              List(
                CellwiseMatrixMatrixTransformation(
                  CellArrayReferenceMatrix(
                    CellArrayExecutable(
                      List(
                        eye(
                          scalar(2.0),
                          scalar(10.0)
                        ),
                        ones(
                          scalar(10.0),
                          scalar(2.0)
                        )
                      )
                    ),
                    0,
                    MatrixType(
                      DoubleType,
                      Some(2),
                      Some(10)
                    )
                  ),
                  CellwiseMatrixMatrixTransformation(
                    MatrixMult(
                      Transpose(
                        CellArrayReferenceMatrix(
                          CellArrayExecutable(
                            List(
                              eye(scalar(2.0),scalar(10.0)),
                              ones(scalar(10.0),scalar(2.0))
                            )
                          ),
                          1,
                          MatrixType(
                            DoubleType,
                            Some(10),
                            Some(2)
                          )
                        )
                      ),
                      eye(scalar(10.0),scalar(10.0))
                    ),
                    MatrixMult(
                      MatrixMult(
                        Transpose(
                          CellArrayReferenceMatrix(
                            CellArrayExecutable(
                              List(
                                eye(scalar(2.0),scalar(10.0)),
                                ones(scalar(10.0),scalar(2.0))
                              )
                            ),
                            1,
                            MatrixType(
                              DoubleType,
                              Some(10),
                              Some(2)
                            )
                          )
                        ),
                        CellArrayReferenceMatrix(
                          CellArrayExecutable(
                            List(
                              eye(scalar(2.0),scalar(10.0)),
                              ones(scalar(10.0),scalar(2.0))
                            )
                          ),
                          1,
                          MatrixType(
                            DoubleType,
                            Some(10),
                            Some(2)
                          )
                        )
                      ),
                      CellArrayReferenceMatrix(
                        CellArrayExecutable(
                          List(
                            eye(scalar(2.0),scalar(10.0)),
                            ones(scalar(10.0),scalar(2.0))
                          )
                        ),
                        0,
                        MatrixType(
                          DoubleType,
                          Some(2),
                          Some(10)
                        )
                      )
                    ),
                    Division
                  ),
                  Multiplication
                ),
                CellwiseMatrixMatrixTransformation(
                  CellwiseMatrixMatrixTransformation(
                    CellArrayReferenceMatrix(
                      CellArrayExecutable(
                        List(
                          eye(scalar(2.0),scalar(10.0)),
                          ones(scalar(10.0),scalar(2.0))
                        )
                      ),
                      1,
                      MatrixType(
                        DoubleType,
                        Some(10),
                        Some(2)
                      )
                    ),
                    MatrixMult(
                      eye(scalar(10.0),scalar(10.0)),
                      Transpose(
                        CellwiseMatrixMatrixTransformation(
                          CellArrayReferenceMatrix(
                            CellArrayExecutable(
                              List(
                                eye(scalar(2.0),scalar(10.0)),
                                ones(scalar(10.0),scalar(2.0))
                              )
                            ),
                            0,
                            MatrixType(
                              DoubleType,
                              Some(2),
                              Some(10)
                            )
                          ),
                          CellwiseMatrixMatrixTransformation(
                            MatrixMult(
                              Transpose(
                                CellArrayReferenceMatrix(
                                  CellArrayExecutable(
                                    List(
                                      eye(scalar(2.0),scalar(10.0)),
                                      ones(scalar(10.0),scalar(2.0))
                                    )
                                  ),
                                  1,
                                  MatrixType(
                                    DoubleType,
                                    Some(10),
                                    Some(2)
                                  )
                                )
                              ),
                              eye(scalar(10.0),scalar(10.0))
                            ),
                            MatrixMult(
                              MatrixMult(
                                Transpose(
                                  CellArrayReferenceMatrix(
                                    CellArrayExecutable(
                                      List(
                                        eye(scalar(2.0),scalar(10.0)),
                                        ones(scalar(10.0),scalar(2.0))
                                      )
                                    ),
                                    1,
                                    MatrixType(
                                      DoubleType,
                                      Some(10),
                                      Some(2)
                                    )
                                  )
                                ),
                                CellArrayReferenceMatrix(
                                  CellArrayExecutable(
                                    List(
                                      eye(scalar(2.0),scalar(10.0)),
                                      ones(scalar(10.0),scalar(2.0))
                                    )
                                  ),
                                  1,
                                  MatrixType(
                                    DoubleType,
                                    Some(10),
                                    Some(2)
                                  )
                                )
                              ),
                              CellArrayReferenceMatrix(
                                CellArrayExecutable(
                                  List(
                                    eye(scalar(2.0),scalar(10.0)),
                                    ones(scalar(10.0),scalar(2.0))
                                  )
                                ),
                                0,
                                MatrixType(
                                  DoubleType,
                                  Some(2),
                                  Some(10)
                                )
                              )
                            ),
                            Division
                          ),
                          Multiplication
                        )
                      )
                    ),
                    Multiplication
                  ),
                  MatrixMult(
                    MatrixMult(
                      CellArrayReferenceMatrix(
                        CellArrayExecutable(
                          List(
                            eye(scalar(2.0),scalar(10.0)),
                            ones(scalar(10.0),scalar(2.0))
                          )
                        ),
                        1,
                        MatrixType(
                          DoubleType,
                          Some(10),
                          Some(2)
                        )
                      ),
                      CellwiseMatrixMatrixTransformation(
                        CellArrayReferenceMatrix(
                          CellArrayExecutable(
                            List(
                              eye(scalar(2.0),scalar(10.0)),
                              ones(scalar(10.0),scalar(2.0))
                            )
                          ),
                          0,
                          MatrixType(
                            DoubleType,
                            Some(2),
                            Some(10)
                          )
                        ),
                        CellwiseMatrixMatrixTransformation(
                          MatrixMult(
                            Transpose(
                              CellArrayReferenceMatrix(
                                CellArrayExecutable(
                                  List(
                                    eye(scalar(2.0),scalar(10.0)),
                                    ones(scalar(10.0),scalar(2.0))
                                  )
                                ),
                                1,
                                MatrixType(
                                  DoubleType,
                                  Some(10),
                                  Some(2)
                                )
                              )
                            ),
                            eye(scalar(10.0),scalar(10.0))
                          ),
                          MatrixMult(
                            MatrixMult(
                              Transpose(
                                CellArrayReferenceMatrix(
                                  CellArrayExecutable(
                                    List(
                                      eye(scalar(2.0),scalar(10.0)),
                                      ones(scalar(10.0),scalar(2.0))
                                    )
                                  ),
                                  1,
                                  MatrixType(
                                    DoubleType,
                                    Some(10),
                                    Some(2)
                                  )
                                )
                              ),
                              CellArrayReferenceMatrix(
                                CellArrayExecutable(
                                  List(
                                    eye(scalar(2.0),scalar(10.0)),
                                    ones(scalar(10.0),scalar(2.0))
                                  )
                                ),
                                1,
                                MatrixType(
                                  DoubleType,
                                  Some(10),
                                  Some(2)
                                )
                              )
                            ),
                            CellArrayReferenceMatrix(
                              CellArrayExecutable(
                                List(
                                  eye(scalar(2.0),scalar(10.0)),
                                  ones(scalar(10.0),scalar(2.0))
                                )
                              ),
                              0,
                              MatrixType(
                                DoubleType,
                                Some(2),
                                Some(10)
                              )
                            )
                          ),
                          Division
                        ),
                        Multiplication
                      )
                    ),
                    Transpose(
                      CellwiseMatrixMatrixTransformation(
                        CellArrayReferenceMatrix(
                          CellArrayExecutable(
                            List(
                              eye(scalar(2.0),scalar(10.0)),
                              ones(scalar(10.0),scalar(2.0))
                            )
                          ),
                          0,
                          MatrixType(
                            DoubleType,
                            Some(2),
                            Some(10)
                          )
                        ),
                        CellwiseMatrixMatrixTransformation(
                          MatrixMult(
                            Transpose(
                              CellArrayReferenceMatrix(
                                CellArrayExecutable(
                                  List(
                                    eye(scalar(2.0),scalar(10.0)),
                                    ones(scalar(10.0),scalar(2.0))
                                  )
                                ),
                                1,
                                MatrixType(
                                  DoubleType,
                                  Some(10),
                                  Some(2)
                                )
                              )
                            ),
                            eye(scalar(10.0),scalar(10.0))
                          ),
                          MatrixMult(
                            MatrixMult(
                              Transpose(
                                CellArrayReferenceMatrix(
                                  CellArrayExecutable(
                                    List(
                                      eye(scalar(2.0),scalar(10.0)),
                                      ones(scalar(10.0),scalar(2.0))
                                    )
                                  ),
                                  1,
                                  MatrixType(
                                    DoubleType,
                                    Some(10),
                                    Some(2)
                                  )
                                )
                              ),
                              CellArrayReferenceMatrix(
                                CellArrayExecutable(
                                  List(
                                    eye(scalar(2.0),scalar(10.0)),
                                    ones(scalar(10.0),scalar(2.0))
                                  )
                                ),
                                1,
                                MatrixType(
                                  DoubleType,
                                  Some(10),
                                  Some(2)
                                )
                              )
                            ),
                            CellArrayReferenceMatrix(
                              CellArrayExecutable(
                                List(
                                  eye(scalar(2.0),scalar(10.0)),
                                  ones(scalar(10.0),scalar(2.0))
                                )
                              ),
                              0,
                              MatrixType(
                                DoubleType,
                                Some(2),
                                Some(10)
                              )
                            )
                          ),
                          Division
                        ),
                        Multiplication
                      )
                    )
                  ),
                  Division
                )
              )
            )
          )
        )
      )

    val result = Gilbert.compileRessource(filename)

    expectResult(expected)(result)
  }

  @Test def testGeneralization(){
    val filename = "testGeneralization.gb"
    val expected = CompoundExecutable(List(WriteMatrix(zeros(scalar(10.0),scalar(10.0))), WriteScalar(boolean(value = true))))

    val result = Gilbert.compileRessource(filename)

    expectResult(expected)(result)
  }

  @Test def testTypeWidening(){
    val filename = "testTypeWidening.gb"
    val expected = CompoundExecutable(
      List(
        WriteScalar(
          ScalarScalarTransformation(
            scalar(1.0),
            TypeConversionScalar(
              boolean(value = true),
              BooleanType,
              DoubleType
            ),
            Addition
          )
        ),
        WriteScalar(
          ScalarScalarTransformation(
            scalar(1.0),
            TypeConversionScalar(
              boolean(value = false),
              BooleanType,
              DoubleType
            ),
            Addition
          )
        ),
        WriteScalar(
          ScalarScalarTransformation(
            scalar(1.0),
            TypeConversionScalar(
              boolean(value = false),
              BooleanType,
              DoubleType
            ),
            Addition
          )
        ),
        WriteMatrix(
          ones(
            scalar(2.0),
            scalar(2.0)
          )
        ),
        WriteMatrix(
          MatrixScalarTransformation(
            ones(
              scalar(2.0),
              scalar(2.0)
            ),
            scalar(0.0),
            GreaterThan
          )
        ),
        WriteMatrix(
          MatrixMult(
            ones(
              scalar(2.0),
              scalar(2.0)
            ),
            TypeConversionMatrix(
              MatrixScalarTransformation(
                ones(
                  scalar(2.0),
                  scalar(2.0)
                ),
                scalar(0.0),
                GreaterThan
              ),
              MatrixType(
                BooleanType,
                Some(2),
                Some(2)
              ),
              MatrixType(
                DoubleType,
                Some(2),
                Some(2)
              )
            )
          )
        )
      )
    )
    val result = Gilbert.compileRessource(filename)
    expectResult(expected)(result)
  }
}

