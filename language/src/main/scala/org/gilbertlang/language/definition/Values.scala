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

object Values {

  object Helper {

    private var valueVarCounter = 0

    def uv(value: ValueVar) = UniversalValue(value)
    def uvv = UniversalValue(newVV())
    def newVV() = {
      val result = ValueVar(valueVarCounter)
      valueVarCounter += 1
      result
    }
  }
  abstract class Value
  case class ValueVar(id: Int = -1) extends Value
  case class IntValue(value: Int) extends Value
  //case class ExpressionValue(value: TypedExpression) extends MValue
  case class ReferenceValue(reference: Int) extends Value
  case class UniversalValue(value: ValueVar) extends Value{
//    override def equals(uv: Any): Boolean = {
//      (uv,this) match{
//        case (UniversalValue(x:ValueVar), UniversalValue(y:ValueVar)) => true
//        case (UniversalValue(x), UniversalValue(y)) => x == y
//        case _ => false
//      }
//    }
//    
//    override def hashCode():Int = {
//      this match{
//        case UniversalValue(x:ValueVar) => 41*ValueVar().hashCode + ValueVar().hashCode
//        case _ => super.hashCode
//      }
//    }
  }
  case object UndefinedValue extends Value
}