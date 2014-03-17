package org.gilbertlang.language.definition

import org.gilbertlang.language.definition.Operators._
import org.gilbertlang.language.definition.ConvenienceMethods._
import org.gilbertlang.language.definition.Types._
import org.gilbertlang.language.definition.Values.Helper._
import org.gilbertlang.language.definition.Types.Helper._
import org.gilbertlang.language.definition.Types.PolymorphicType
import org.gilbertlang.language.definition.Types.MatrixType


object BuiltinOperators extends AbstractBuiltinSymbols[Operator] {
  Symbol(GTOp, gtType)
  Symbol(GTEOp,gteType)
  Symbol(LTOp, ltType)
  Symbol(LTEOp, lteType)
  Symbol(DEQOp, deqType)
  Symbol(NEQOp, neqType)
  Symbol(TransposeOp, transposeType)
  Symbol(CellwiseTransposeOp, cellwiseTransposeType)
  Symbol(PrePlusOp, prePlusType)
  Symbol(PreMinusOp, preMinusType)
  Symbol(ExpOp, expType)
  Symbol(CellwiseExpOp, cellwiseExpType)
  Symbol(PlusOp, plusType)
  Symbol(MinusOp, minusType)
  Symbol(MultOp, multType)
  Symbol(DivOp, divType)
  Symbol(CellwiseMultOp, cellwiseMultType)
  Symbol(CellwiseDivOp, cellwiseDivType)
  Symbol(LogicalAndOp, logicalAndType)
  Symbol(LogicalOrOp, logicalOrType)
  Symbol(ShortCircuitLogicalAndOp, shortCircuitLogicalAndType)
  Symbol(ShortCircuitLogicalOrOp, shortCircuitLogicalOrType)

  def transposeType = {
    val (t,a,b) = newUTVV()
    val nt = utv
    PolymorphicType(List(FunctionType(MatrixType(t,a,b), MatrixType(t,b,a)),
    FunctionType(CharacterType, CharacterType),
    FunctionType(nt, nt),
    FunctionType(BooleanType, BooleanType)))
  }

  def cellwiseTransposeType = transposeType

  def prePlusType = {
    val (t,a,b) = newUNTVV()
    PolymorphicType(List(FunctionType(MatrixType(t,a,b), MatrixType(t,a,b)),
    FunctionType(t, t)))
  }

  def preMinusType = prePlusType

  def expType = {
    val a = uvv
    val t = untv
    PolymorphicType(List(FunctionType((MatrixType(t,a,a), DoubleType), MatrixType(t, a, a)),
    FunctionType((DoubleType, DoubleType), DoubleType)))
  }

  def cellwiseExpType = {
    val(t,a,b) = newUNTVV()
    PolymorphicType(List(FunctionType((MatrixType(t,a,b), DoubleType), MatrixType(t,a,b)),
    FunctionType((DoubleType, DoubleType), DoubleType)))
  }

  def plusType = {
    val (t,a,b) = newUNTVV()
    PolymorphicType(List(FunctionType((MatrixType(t,a,b), MatrixType(t,a,b)), MatrixType(t,a,b)),
    FunctionType((MatrixType(t,a,b), t), MatrixType(t,a,b)),
    FunctionType((t, MatrixType(t, a,b)), MatrixType(t,a,b)),
    FunctionType((t,t), t)))
  }

  def minusType = plusType

  def multType = {
    val (t,a,b) = newUNTVV()
    val c = uvv()
    PolymorphicType(List(FunctionType((MatrixType(t,a,b), MatrixType(t,b,c)), MatrixType(t,a,c)),
    FunctionType((MatrixType(t,a,b), t), MatrixType(t,a,b)),
    FunctionType((t, MatrixType(t,a,b)), MatrixType(t,a,b)),
    FunctionType((t,t),t)
    ))
  }

  def divType = {
    val (t,a,b) = newUNTVV()
    PolymorphicType(List(FunctionType((MatrixType(t,a,b), t), MatrixType(t,a,b)),
    FunctionType((DoubleType, DoubleType), DoubleType)))
  }

  def cellwiseMultType = {
    val (t,a,b) = newUNTVV()
    PolymorphicType(List(FunctionType((MatrixType(t,a,b), MatrixType(t,a,b)), MatrixType(t,a,b)),
    FunctionType((MatrixType(t,a,b), t), MatrixType(t,a,b)),
    FunctionType((t, MatrixType(t,a,b)), MatrixType(t,a,b)),
    FunctionType((t,t),t)))
  }

  def cellwiseDivType = cellwiseMultType

  def logicalOrType = {
    val a = uvv()
    val b = uvv()
    PolymorphicType(List(FunctionType((MatrixType(BooleanType, a,b), MatrixType(BooleanType, a,b)), MatrixType(BooleanType, a,b)),
    FunctionType((MatrixType(BooleanType, a,b), BooleanType), MatrixType(BooleanType, a,b)),
    FunctionType((BooleanType, MatrixType(BooleanType, a,b)), MatrixType(BooleanType, a,b)),
    FunctionType((BooleanType, BooleanType), BooleanType)
    ))
  }

  def logicalAndType = logicalOrType
  def shortCircuitLogicalAndType = logicalOrType
  def shortCircuitLogicalOrType = logicalOrType

  def gtType = {
    val (t,a,b) = newUNTVV()
    PolymorphicType(List(FunctionType((MatrixType(t,a,b), MatrixType(t,a,b)), MatrixType(BooleanType, a,b)),
    FunctionType((MatrixType(t,a,b), t), MatrixType(BooleanType, a,b)),
    FunctionType((t, MatrixType(t,a,b)), MatrixType(BooleanType, a,b)),
    FunctionType((t,t), BooleanType)))
  }

  def gteType = gtType
  def ltType = gtType
  def lteType = gtType
  def deqType = gtType
  def neqType = gtType
}
