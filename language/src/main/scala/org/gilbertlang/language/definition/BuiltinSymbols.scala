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
object BuiltinSymbols extends AbstractBuiltinSymbols[String] {
  
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
  val rand = Symbol("rand", randType)
  val zeros = Symbol("zeros", zerosType)
  val eye = Symbol("eye", eyeType)
  val norm = Symbol("norm",normType)
  val repmat = Symbol("repmat", repmatType)
  val linspace = Symbol("linspace", linspaceType)
  val minWithIndex = Symbol("minWithIndex", minWithIndexType)
  val pdist2 = Symbol("pdist2", pdist2Type)
  val abs = Symbol("abs", absType)
  val sprand = Symbol("sprand", sprandType)

  def sprandType = {
    FunctionType((DoubleType, DoubleType, DoubleType, DoubleType, DoubleType), MatrixType(DoubleType,
      ReferenceValue(0), ReferenceValue(1)))
  }

  def absType = {
    val nt = newNumericTV()
    FunctionType(nt, nt)
  }

  def repmatType = {
    val (t,a,b) = newUTVV()
    FunctionType((MatrixType(t,a,b), DoubleType, DoubleType), MatrixType(t,UndefinedValue, UndefinedValue))
  }

  def linspaceType = {
    FunctionType((DoubleType, DoubleType, DoubleType), MatrixType(DoubleType, IntValue(1), ReferenceValue(2)))
  }

  def minWithIndexType = {
    val (t,a,b) = newUNTVV()
    FunctionType((MatrixType(t,a,b), DoubleType), ConcreteCellArrayType(List(MatrixType(t,UndefinedValue,
      UndefinedValue), MatrixType(DoubleType,UndefinedValue, UndefinedValue))))
  }

  def pdist2Type = {
    val (t,a,b) = newUNTVV()
    val c = uvv()
    FunctionType((MatrixType(t,a,b), MatrixType(t,c,b)), MatrixType(t,a,c))
  }

  def normType = {
    val (t,a,b) = newUNTVV()
    FunctionType((MatrixType(t,a,b),DoubleType), DoubleType)
  }
  
  def eyeType = {
    FunctionType((DoubleType, DoubleType), MatrixType(DoubleType, ReferenceValue(0), ReferenceValue(1)))
  }
  
  def zerosType = {
    FunctionType((DoubleType, DoubleType), MatrixType(DoubleType, ReferenceValue(0), ReferenceValue(1)))
  }
  
  def randType = {
    PolymorphicType(List(
      FunctionType((DoubleType, DoubleType, DoubleType, DoubleType), MatrixType(DoubleType, ReferenceValue(0),
          ReferenceValue(1))),
      FunctionType((DoubleType, DoubleType, DoubleType, DoubleType, DoubleType), MatrixType(DoubleType,
        ReferenceValue(0), ReferenceValue(1)))
    ))
  }

  def loadType = {
    FunctionType((StringType, DoubleType, DoubleType), MatrixType(DoubleType, ReferenceValue(1), ReferenceValue(2)))
  }

  def binarizeType = {
    val a = uvv()
    val b = uvv()
    PolymorphicType(List(
      FunctionType(MatrixType(DoubleType, a, b), MatrixType(DoubleType, a, b)),
      FunctionType(DoubleType, DoubleType)))
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
    val (t, a, b) = newUNTVV()
    PolymorphicType(List(
        FunctionType(MatrixType(t, a, IntValue(1)), MatrixType(t, IntValue(1), IntValue(1))),
        FunctionType(MatrixType(t, IntValue(1), a), MatrixType(t, IntValue(1), IntValue(1))),
        FunctionType((MatrixType(t, a, b), DoubleType), MatrixType(t, UndefinedValue, UndefinedValue))))
  }
  
  def sumRowType = {
    val (t, a, b) = newUTVV()
    val (t1, a1) = (utv(), uvv())
    val (t2, a2) = (utv(), uvv())
    PolymorphicType(List(
        FunctionType(MatrixType(t1, a1, IntValue(1)),MatrixType(t1, IntValue(1), IntValue(1))),
        FunctionType(MatrixType(t2, IntValue(1),a2),MatrixType(t2, IntValue(1), IntValue(1))),
        FunctionType(MatrixType(t, a, b), MatrixType(t, a, IntValue(1)))))
  }
  
   def sumColType = {
    val (t, a, b) = newUTVV()
    val (t1, a1) = (utv(), uvv())
    val (t2, a2) = (utv(), uvv())
    PolymorphicType(List(
        FunctionType(MatrixType(t1, a1, IntValue(1)), MatrixType(t1, IntValue(1), IntValue(1))),
        FunctionType(MatrixType(t2, IntValue(1), a2), MatrixType(t2, IntValue(1), IntValue(1))),
        FunctionType(MatrixType(t, a, b), MatrixType(t, IntValue(1), b))))
  }
  
  def diagType = {
    val t = untv
    val a = uvv()
    val t1 = untv
    val a1 = uvv()
    val t2 = untv
    val a2 = uvv()
    PolymorphicType(List(
        FunctionType(MatrixType(t, a, IntValue(1)), MatrixType(t, a, a)),
        FunctionType(MatrixType(t1, IntValue(1), a1), MatrixType(t1, a1, a1)),
        FunctionType(MatrixType(t2, a2, a2), MatrixType(t2, a2, IntValue(1)))))
  }
  
  def onesType = {
    PolymorphicType(List(
        FunctionType(DoubleType, MatrixType(DoubleType, ReferenceValue(0),ReferenceValue(0))),
        FunctionType((DoubleType, DoubleType), MatrixType(DoubleType, ReferenceValue(0), ReferenceValue(1)))))
  }
  
  def fixpointType = {
    val t = utv()
    PolymorphicType(List(
    FunctionType((t, FunctionType(t, t), DoubleType,FunctionType((t,t), BooleanType)), t),
    FunctionType((t, FunctionType(t,t), DoubleType), t)))
  }
  
  def writeType = {
    val (t, a, b) = newUTVV()
    PolymorphicType(List(FunctionType((MatrixType(t, a, b), StringType), VoidType)))
  }

}