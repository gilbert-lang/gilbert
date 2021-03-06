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

package org.gilbertlang.language.format

import org.gilbertlang.language.definition.Types._

class TypeFormatter extends Formatter[Type] {
  private val valueFormatter = new ValueFormatter{}
  def prettyString(mtype: Type): String = {

    val result = mtype match {
      case DoubleType => "Double"
      case CharacterType => "Char"
      case StringType => "String"
      case VoidType => "Void"
      case BooleanType => "Boolean"
      case ConcreteCellArrayType(types) =>
        "ConcreteCellArrayType(" + (types map prettyString mkString ", ") + ")"
      case InterimCellArrayType(types) =>
        "InterimCellArrayType(" + (types map prettyString mkString ", ") + ")"
      case UndefinedType => "Undefined"
      case PolymorphicType(types) =>
        val concatenatedTypes = types map { prettyString } mkString ", "
        "(" + concatenatedTypes + ")"
      case FunctionType(args, resultType) =>
        val concatenatedArgs = args map { prettyString } mkString ", "
        val resultTypeStr = prettyString(resultType)
        "(" + concatenatedArgs + ") => " + resultTypeStr
      case MatrixType(elementType, rows, cols) =>
        val elementTypeStr = prettyString(elementType)
        val rowsStr = valueFormatter.prettyString(rows)
        val colsStr = valueFormatter.prettyString(cols)

        "Matrix[" + elementTypeStr + ", " + rowsStr + ", " + colsStr + "]"
      case NumericTypeVar(id) => "ω(" + id + ")"
      case TypeVar(id) => "𝛕("+id+ ")"
      case UniversalType(valueVar) => "∀" + prettyString(valueVar)
    }
    result
  }
}