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

import org.gilbertlang.language.definition.Values.Value

object Types {

  object Helper {

    private var typeVarCounter:Int = 0

    def mt(elementType: Type, rows: Value, columns:Value) = MatrixType(elementType,rows,columns)
    def pt(types: List[FunctionType]) = PolymorphicType(types)
    def utv() = UniversalType(newTV())
    def untv = UniversalType(newNumericTV())
    
    def newTV() = {
      val result = TypeVar(typeVarCounter)
      typeVarCounter += 1
      result
    }
    
    def newNumericTV() ={
      val result = NumericTypeVar(typeVarCounter)
      typeVarCounter += 1
      result
    }
  }
  
  private val wideableTypes = scala.collection.immutable.Map[Type, List[Type]](
    IntegerType -> List(DoubleType),
    CharacterType -> List(DoubleType, IntegerType),
    BooleanType -> List(DoubleType, IntegerType)
  )

  sealed trait Type {
    def isWideableTo(other: Type): Boolean = {
      Type.this == other || (wideableTypes.getOrElse(Type.this, List()) contains (other))
    }
  }

  case object StringType extends Type
  case object CharacterType extends Type
  case object BooleanType extends Type

  sealed trait NumericType extends Type
  case object IntegerType extends NumericType
  case object DoubleType extends NumericType



  case class FunctionType(parameters: List[Type], value: Type) extends Type {
    def this(parameter: Type, value: Type) = this(List(parameter), value)
  }

  case class MatrixType(elementType: Type, rows: Value, columns: Value) extends Type
  
  sealed trait AbstractTypeVar extends Type
  case class NumericTypeVar(id: Int = -1) extends AbstractTypeVar with NumericType
  case class TypeVar(id: Int = -1) extends AbstractTypeVar
  case class UniversalType(universalType: AbstractTypeVar) extends Type
  
  case class PolymorphicType(types:List[Type]) extends Type

  case object VoidType extends Type
  case object UndefinedType extends Type
  
  object FunctionType {
    def apply(parameter: Type, value: Type) = new FunctionType(parameter, value)
    def apply(parameters: (Type, Type), value: Type) = new FunctionType(List(parameters._1, parameters._2), value)
    def apply(parameters: (Type, Type, Type), value: Type) = {
      new FunctionType(List(parameters._1, parameters._2,parameters._3), value)
    }
    def apply(parameters: (Type, Type, Type, Type), value: Type) = {
      new FunctionType(List(parameters._1, parameters._2, parameters._3, parameters._4), value)
    }
  }
}