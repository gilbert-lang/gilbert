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
import Executables._
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

@SerialVersionUID(1l)
trait Executor extends Serializable {
  @transient private var symbolTable = Map[Int, Any]()
  @transient private var volatileExpressions = Set[Int]()

  implicit var configuration: RuntimeConfiguration = null
  
  protected def clear() {
    symbolTable = Map[Int,Any]()
    volatileExpressions = Set[Int]()
  }

  def run(executable: Executable, configuration: RuntimeConfiguration) = {
    this.configuration = configuration
    symbolTable = Map[Int, Any]()
    volatileExpressions = new VolatileExpressionDetector().find(executable)

    printPlan(executable)

    val result = execute(executable)

    result
  }

  protected def execute(transformation: Executable): Any

  def evaluate[T](in: Executable) = {
    execute(in).asInstanceOf[T]
  }

  def evaluate[T](in: List[Executable]): List[T] = {
    in map { x => execute(x).asInstanceOf[T]}
  }

  def handle[T <: Executable, I](executable: T, retrieveInput: (T) => I, handle: (T, I) => Any): Any = {

    val id = executable.id

    /* check if we already processed this expression */
    if (symbolTable.contains(id)) {
      println("\t reusing (" + id + ")")
      return symbolTable(id)
    }

    val input = retrieveInput(executable)
    println("\t executing (" + id + ") ")

    val output = handle(executable, input)

    if (!volatileExpressions.contains(id)) {
      symbolTable += (id -> output)
    }

    output
  }
}
