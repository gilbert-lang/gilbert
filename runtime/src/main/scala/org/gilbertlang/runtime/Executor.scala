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

package org.gilbertlang.runtime

import org.gilbertlang.runtime.optimization.VolatileExpressionDetector
import org.gilbertlang.runtime.shell.printPlan
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import Executables._

@SerialVersionUID(1l)
trait Executor extends Serializable {

  private var symbolTable = Map[Int, Any]()
  
  private var volatileExpressions = Set[Int]()
  
  protected def clear() {
    symbolTable = Map[Int,Any]()
    volatileExpressions = Set[Int]()
  }

  def run(executable: Executable) = {

    volatileExpressions = new VolatileExpressionDetector().find(executable)

    printPlan(executable)

    val result = execute(executable)
    
    //TODO: Workaround for serialization problem with symbolTable and volatileExpressions, at the moment there is no
    //way to declare them transient in Scala 2.10
    clear()
    
    result
  }

  protected def execute(transformation: Executable): Any

  def evaluate[T](in: Executable) = {
    execute(in).asInstanceOf[T]
  }

  def handle[T <: Executable, I](executable: T, retrieveInput: (T) => I, handle: (T, I) => Any): Any = {

    val id = executable.id

    /* check if we already processed this expression */
    if (symbolTable.contains(id)) {
      println("\t reusing (" + id + ")")
      return symbolTable(id)
    }

    val input = retrieveInput(executable)
    println("\t executing (" + id + ") " + executable)

    val output = handle(executable, input)

    if (!volatileExpressions.contains(id)) {
      symbolTable += (id -> output)
    }

    output
  }
}
