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

import org.gilbertlang.runtime.execution.reference.ReferenceExecutor
import org.gilbertlang.runtime.execution.spark.SparkExecutor
import eu.stratosphere.client.LocalExecutor
import org.gilbertlang.runtime.execution.stratosphere.StratosphereExecutor

object local {
  def apply(executable: Executable) = {

    val write = executable match {
      case matrix: Matrix => WriteMatrix(matrix)
      case scalar: ScalarRef => WriteScalarRef(scalar)
      case string: StringRef => WriteString(string)
      case function: FunctionRef => WriteFunction(function)
      case _ => executable
    }

    new ReferenceExecutor().run(write)
  }
}

object withSpark {
  def apply(executable: Executable) = {

    val write = executable match {
      case matrix: Matrix => WriteMatrix(matrix)
      case scalar: ScalarRef => WriteScalarRef(scalar)
      case string: StringRef => WriteString(string)
      case function: FunctionRef => WriteFunction(function)
      case _ => executable
    }

    new SparkExecutor().run(write)
  }
}

object withStratosphere{
  def apply(executable: Executable) = {
    val write = executable match {
      case matrix: Matrix => WriteMatrix(matrix)
      case scalar: ScalarRef => WriteScalarRef(scalar)
      case string: StringRef => WriteString(string)
      case function: FunctionRef => WriteFunction(function)
      case _ => executable
    }
    new StratosphereExecutor().run(write)
  }
}
