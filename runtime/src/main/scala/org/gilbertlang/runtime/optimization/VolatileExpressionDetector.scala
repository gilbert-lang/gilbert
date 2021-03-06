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

package org.gilbertlang.runtime.optimization

import org.gilbertlang.runtime.Executables._
import scala.collection.mutable.Stack
import scala.collection.mutable

class VolatileExpressionDetector extends DagWalker {

  private val volatileExpressions = scala.collection.mutable.Set[Int]()
  private val stack = mutable.Stack[Boolean]()

  def find(executable: Executable): Set[Int] = {
    visit(executable)
    volatileExpressions.toSet
  }

  override def onArrival(transformation: Executable) {
    transformation match {
      case _: Placeholder =>
      case _ => stack.push(false)
    }
  }

  override def onLeave(transformation: Executable) {
    transformation match {
      case _: FixpointBase =>
        val volatile = stack.pop()
        if (volatile) {
          volatileExpressions.add(transformation.id)
        }
      case placeholder: Placeholder =>
        volatileExpressions.add(placeholder.id)
        stack.pop()
        stack.push(true)
      case _ =>
        val volatile = stack.pop()

        if (volatile) {
          volatileExpressions.add(transformation.id)
          stack.pop()
          stack.push(true)
        }
    }
  }
}