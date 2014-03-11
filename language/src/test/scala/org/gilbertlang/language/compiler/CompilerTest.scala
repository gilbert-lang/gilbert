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
import org.gilbertlang.runtime.RuntimeTypes.{ScalarType, MatrixType, CellArrayType}
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
import org.gilbertlang.runtime.RuntimeTypes.MatrixType
import org.gilbertlang.runtime.Executables.WriteScalar
import org.gilbertlang.runtime.Executables.function
import org.gilbertlang.runtime.Executables.zeros
import org.gilbertlang.runtime.RuntimeTypes.CellArrayType
import org.gilbertlang.runtime.Executables.eye
import org.gilbertlang.runtime.Executables.FixpointIteration
import org.gilbertlang.runtime.Executables.CellArrayReferenceMatrix
import org.gilbertlang.runtime.Executables.ones
import org.gilbertlang.runtime.Executables.randn
import org.gilbertlang.runtime.Executables.WriteFunction
import org.gilbertlang.runtime.Executables.norm
import org.gilbertlang.runtime.Executables.CompoundExecutable

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
    val expected = CompoundExecutable(List(WriteCellArray(CellArrayExecutable(List(boolean(true),
      zeros(scalar(10.0),scalar(10.0))))), WriteMatrix(zeros(scalar(10.0),scalar(10.0)))))
    val filename = "testCellArrayCompilation.gb"

    val result = Gilbert.compileRessource(filename)

    expectResult(expected)(result)
  }

  @Test def testCellArrayAnonymousFunctionCompilation(){

    val expected = CompoundExecutable(List(WriteFunction(function(1,CellwiseMatrixMatrixTransformation(
      CellArrayReferenceMatrix(CellArrayParameter(0,CellArrayType(List(MatrixType(ScalarType),
        MatrixType(ScalarType)))),0),CellArrayReferenceMatrix(CellArrayParameter(0,
        CellArrayType(List( MatrixType(ScalarType), MatrixType(ScalarType)))),1),Addition))),
      WriteMatrix(CellwiseMatrixMatrixTransformation(CellArrayReferenceMatrix(CellArrayExecutable(List( zeros(scalar
        (1.0),scalar(1.0)), ones(scalar(1.0),scalar(1.0)))),0), CellArrayReferenceMatrix(CellArrayExecutable(List
        (zeros(scalar(1.0),scalar(1.0)), ones(scalar(1.0),scalar(1.0)))),1),Addition))))

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
    val expected = CompoundExecutable(
      List(
        WriteCellArray(
          CellArrayExecutable(
            List(
              CellwiseMatrixMatrixTransformation(
                CellwiseMatrixMatrixTransformation(
                  ScalarMatrixTransformation(
                    scalar(0.1),
                    ones(
                      scalar(2.0),
                      scalar(10.0)
                    ),
                    Multiplication
                  ),
                  eye(
                    scalar(2.0),
                    scalar(10.0)
                  ),
                  Addition
                ),
                CellwiseMatrixMatrixTransformation(
                  MatrixMult(
                    Transpose(
                      ScalarMatrixTransformation(
                        scalar(0.5),
                        ones(
                          scalar(10.0),
                          scalar(2.0)
                        ),
                        Multiplication
                      )
                    ),
                    CellwiseMatrixMatrixTransformation(
                      eye(
                        scalar(10.0),
                        scalar(10.0)
                      ),
                      ScalarMatrixTransformation(
                        scalar(0.1),
                        ones(
                          scalar(10.0),
                          scalar(10.0)
                        ),
                        Multiplication
                      ),
                      Addition
                    )
                  ),
                  MatrixMult(
                    MatrixMult(
                      Transpose(
                        ScalarMatrixTransformation(
                          scalar(0.5),
                          ones(
                            scalar(10.0),
                            scalar(2.0)
                          ),
                          Multiplication
                        )
                      ),
                      ScalarMatrixTransformation(
                        scalar(0.5),
                        ones(
                          scalar(10.0),
                          scalar(2.0)
                        ),
                        Multiplication
                      )
                    ),
                    CellwiseMatrixMatrixTransformation(
                      ScalarMatrixTransformation(
                        scalar(0.1),
                        ones(
                          scalar(2.0),
                          scalar(10.0)
                        ),
                        Multiplication
                      ),
                      eye(
                        scalar(2.0),
                        scalar(10.0)
                      ),
                      Addition
                    )
                  ),
                  Division
                ),
                Multiplication
              ),
              CellwiseMatrixMatrixTransformation(
                CellwiseMatrixMatrixTransformation(
                  ScalarMatrixTransformation(
                    scalar(0.5),
                    ones(
                      scalar(10.0),
                      scalar(2.0)
                    ),
                    Multiplication
                  ),
                  MatrixMult(
                    CellwiseMatrixMatrixTransformation(
                      eye(
                        scalar(10.0),
                        scalar(10.0)
                      ),
                      ScalarMatrixTransformation(
                        scalar(0.1),
                        ones(
                          scalar(10.0),
                          scalar(10.0)
                        ),
                        Multiplication
                      ),
                      Addition
                    ),
                    Transpose(
                      CellwiseMatrixMatrixTransformation(
                        CellwiseMatrixMatrixTransformation(
                          ScalarMatrixTransformation(
                            scalar(0.1),
                            ones(
                              scalar(2.0),
                              scalar(10.0)
                            ),
                            Multiplication
                          ),
                          eye(
                            scalar(2.0),
                            scalar(10.0)
                          ),
                          Addition
                        ),
                        CellwiseMatrixMatrixTransformation(
                          MatrixMult(
                            Transpose(
                              ScalarMatrixTransformation(
                                scalar(0.5),
                                ones(
                                  scalar(10.0),
                                  scalar(2.0)
                                ),
                                Multiplication
                              )
                            ),
                            CellwiseMatrixMatrixTransformation(
                              eye(
                                scalar(10.0),
                                scalar(10.0)
                              ),
                              ScalarMatrixTransformation(
                                scalar(0.1),
                                ones(
                                  scalar(10.0),
                                  scalar(10.0)
                                ),
                                Multiplication
                              ),
                              Addition
                            )
                          ),
                          MatrixMult(
                            MatrixMult(
                              Transpose(
                                ScalarMatrixTransformation(
                                  scalar(0.5),
                                  ones(
                                    scalar(10.0),
                                    scalar(2.0)
                                  ),
                                  Multiplication
                                )
                              ),
                              ScalarMatrixTransformation(
                                scalar(0.5),
                                ones(
                                  scalar(10.0),
                                  scalar(2.0)
                                ),
                                Multiplication
                              )
                            ),
                            CellwiseMatrixMatrixTransformation(
                              ScalarMatrixTransformation(
                                scalar(0.1),
                                ones(
                                  scalar(2.0),
                                  scalar(10.0)
                                ),
                                Multiplication
                              ),
                              eye(
                                scalar(2.0),
                                scalar(10.0)
                              ),
                              Addition
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
                    ScalarMatrixTransformation(
                      scalar(0.5),
                      ones(
                        scalar(10.0),
                        scalar(2.0)
                      ),
                      Multiplication
                    ),
                    CellwiseMatrixMatrixTransformation(
                      CellwiseMatrixMatrixTransformation(
                        ScalarMatrixTransformation(
                          scalar(0.1),
                          ones(
                            scalar(2.0),
                            scalar(10.0)
                          ),
                          Multiplication
                        ),
                        eye(
                          scalar(2.0),
                          scalar(10.0)
                        ),
                        Addition
                      ),
                      CellwiseMatrixMatrixTransformation(
                        MatrixMult(
                          Transpose(
                            ScalarMatrixTransformation(
                              scalar(0.5),
                              ones(
                                scalar(10.0),
                                scalar(2.0)
                              ),
                              Multiplication
                            )
                          ),
                          CellwiseMatrixMatrixTransformation(
                            eye(
                              scalar(10.0),
                              scalar(10.0)
                            ),
                            ScalarMatrixTransformation(
                              scalar(0.1),
                              ones(
                                scalar(10.0),
                                scalar(10.0)
                              ),
                              Multiplication
                            ),
                            Addition
                          )
                        ),
                        MatrixMult(
                          MatrixMult(
                            Transpose(
                              ScalarMatrixTransformation(
                                scalar(0.5),
                                ones(
                                  scalar(10.0),
                                  scalar(2.0)
                                ),
                                Multiplication
                              )
                            ),
                            ScalarMatrixTransformation(
                              scalar(0.5),
                              ones(
                                scalar(10.0),
                                scalar(2.0)
                              ),
                              Multiplication
                            )
                          ),
                          CellwiseMatrixMatrixTransformation(
                            ScalarMatrixTransformation(
                              scalar(0.1),
                              ones(
                                scalar(2.0),
                                scalar(10.0)
                              ),
                              Multiplication
                            ),
                            eye(
                              scalar(2.0),
                              scalar(10.0)
                            ),
                            Addition
                          )
                        ),
                        Division
                      ),
                      Multiplication
                    )

                  ),
                  Transpose(
                    CellwiseMatrixMatrixTransformation(
                      CellwiseMatrixMatrixTransformation(
                        ScalarMatrixTransformation(
                          scalar(0.1),
                          ones(
                            scalar(2.0),
                            scalar(10.0)
                          ),
                          Multiplication
                        ),
                        eye(
                          scalar(2.0),
                          scalar(10.0)
                        ),
                        Addition
                      ),
                      CellwiseMatrixMatrixTransformation(
                        MatrixMult(
                          Transpose(
                            ScalarMatrixTransformation(
                              scalar(0.5),
                              ones(
                                scalar(10.0),
                                scalar(2.0)
                              ),
                              Multiplication
                            )
                          ),
                          CellwiseMatrixMatrixTransformation(
                            eye(
                              scalar(10.0),
                              scalar(10.0)
                            ),
                            ScalarMatrixTransformation(
                              scalar(0.1),
                              ones(
                                scalar(10.0),
                                scalar(10.0)
                              ),
                              Multiplication
                            ),
                            Addition
                          )
                        ),
                        MatrixMult(
                          MatrixMult(
                            Transpose(
                              ScalarMatrixTransformation(
                                scalar(0.5),
                                ones(
                                  scalar(10.0),
                                  scalar(2.0)
                                ),
                                Multiplication
                              )
                            ),
                            ScalarMatrixTransformation(
                              scalar(0.5),
                              ones(
                                scalar(10.0),
                                scalar(2.0)
                              ),
                              Multiplication
                            )
                          ),
                          CellwiseMatrixMatrixTransformation(
                            ScalarMatrixTransformation(
                              scalar(0.1),
                              ones(
                                scalar(2.0),
                                scalar(10.0)
                              ),
                              Multiplication
                            ),
                            eye(
                              scalar(2.0),
                              scalar(10.0)
                            ),
                            Addition
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
//
//  @Test def testGeneralization{
//    val filename = "testGeneralization.gb"
//    val expected = CompoundExecutable(List())
//
//    val result = Gilbert.compileRessource(filename)
//
//    expectResult(expected)(result)
//  }
}

