/**
 * gilbert - Distributed Linear Algebra on Sparse Matrices
 * Copyright (C) 2013  Sebastian Schelter, Till Rohrmann
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gilbertlang.language.definition

import Types._
import Types.Helper._
import Values._
import Values.Helper._
import ConvenienceMethods._

//TODO needs self explaining variable names
object BuiltinSymbols extends AbstractBuiltinSymbols {
  
  val binarize = Symbol("binarize", binarizeType)
  val load = Symbol("load", loadType)
  val maxValue = Symbol("maxValue", maxValueType)
  val spones = Symbol("spones", sponesType)
  val sum = Symbol("sum", sumType)
  val diag = Symbol("diag", diagType)
  val ones = Symbol("ones", onesType)
  val fixpoint = Symbol("fixpoint", fixpointType)
  val write = Symbol("write", writeType)
  val sumRow = Symbol("sumRow", sumRowType)
  val sumCol = Symbol("sumCol", sumColType)

  def loadType = {
    FunctionType((StringType, IntegerType, IntegerType), MatrixType(DoubleType, ReferenceValue(1), ReferenceValue(2)))
  }

  def binarizeType = {
    val (t, a, b) = newUNTVV()
    val numericType = untv
    PolymorphicType(List(
      FunctionType(MatrixType(t, a, b), MatrixType(IntegerType, a, b)),
      FunctionType(numericType, IntegerType)))
  }

  def maxValueType = {
    val (t, a, b) = newUNTVV()
    val numericType = untv
    PolymorphicType(List(
      FunctionType(MatrixType(t, a, b), t),
      FunctionType((numericType, numericType), numericType)))
  }
  
  def sponesType = {
    val (t, a, b) = newUNTVV()
    FunctionType(MatrixType(t,a,b), MatrixType(t,a,b))
  }
  
  def sumType = {
    val (t, a, b) = newUTVV()
    val t1 = utv
    val a1 = uvv
    val t2 = utv
    val a2 = uvv
    PolymorphicType(List(
        FunctionType(MatrixType(t1, a1, IntValue(1)), MatrixType(t1, IntValue(1), IntValue(1))),
        FunctionType(MatrixType(t2, IntValue(1), a2), MatrixType(t2, IntValue(1), IntValue(1))),
        FunctionType((MatrixType(t, a, b), IntegerType), MatrixType(t, UndefinedValue, UndefinedValue))))
  }
  
  def sumRowType = {
    val (t, a, b) = newUTVV()
    val (t1, a1) = (utv, uvv)
    val (t2, a2) = (utv, uvv)
    PolymorphicType(List(
        FunctionType(MatrixType(t1, a1, IntValue(1)),MatrixType(t1, IntValue(1), IntValue(1))),
        FunctionType(MatrixType(t2, IntValue(1),a2),MatrixType(t2, IntValue(1), IntValue(1))),
        FunctionType(MatrixType(t, a, b), MatrixType(t, a, IntValue(1)))))
  }
  
   def sumColType = {
    val (t, a, b) = newUTVV()
    val (t1, a1) = (utv, uvv)
    val (t2, a2) = (utv, uvv)
    PolymorphicType(List(
        FunctionType(MatrixType(t1, a1, IntValue(1)), MatrixType(t1, IntValue(1), IntValue(1))),
        FunctionType(MatrixType(t2, IntValue(1), a2), MatrixType(t2, IntValue(1), IntValue(1))),
        FunctionType(MatrixType(t, a, b), MatrixType(t, IntValue(1), b))))
  }
  
  def diagType = {
    val t = untv
    val a = uvv
    val t1 = untv
    val a1 = uvv
    val t2 = untv
    val a2 = uvv
    PolymorphicType(List(
        FunctionType(MatrixType(t, a, IntValue(1)), MatrixType(t, a, a)),
        FunctionType(MatrixType(t1, IntValue(1), a1), MatrixType(t1, a1, a1)),
        FunctionType(MatrixType(t2, a2, a2), MatrixType(t2, a2, IntValue(1)))))
  }
  
  def onesType = {
    PolymorphicType(List(
        FunctionType(IntegerType, MatrixType(IntegerType, ReferenceValue(0),ReferenceValue(0))),
        FunctionType((IntegerType, IntegerType), MatrixType(IntegerType, ReferenceValue(0), ReferenceValue(1)))))
  }
  
  def fixpointType = {
    val t = utv
    FunctionType((t, FunctionType(t, t)), t)
  }
  
  def writeType = {
    val (t, a, b) = newUTVV()
    PolymorphicType(List(FunctionType((MatrixType(t, a, b), StringType), VoidType)))
  }

}