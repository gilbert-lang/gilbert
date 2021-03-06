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

package org.gilbertlang

import java.lang.String
import org.gilbertlang.runtime.Executables._

import scala.language.implicitConversions

package object runtime {
  
  import Executables.{scalar, Matrix, FunctionRef, string, ScalarRef, function, MatrixParameter}
  
  implicit def Int2Scalar(value: Int) = scalar(value.toDouble)
  implicit def Double2Scalar(value: Double) = scalar(value)
  implicit def String2StringRef(value: String) = string(value)

  implicit def MatrixMatrix2FunctionRef(func: Matrix => Matrix): FunctionRef = {
    function(1, func(MatrixParameter(0)))
  }

  implicit def MatrixMatrixBoolean2FunctionRef(func: (Matrix, Matrix) => ScalarRef): FunctionRef = {
    function(2, func(MatrixParameter(0), MatrixParameter(1)))
  }
  
  implicit def boolean2Double(b: Boolean): Double = {
    if(b) 1.0 else 0.0
  }
  
  implicit def double2Boolean(d: Double): Boolean = {
    if(d == 0) false else true
  }


  def scalarRef2Int(scalarRef: ScalarRef) = {
    scalarRef match {
      case scalar(value) => Some(value.toInt)
      case _ => None
    }
  }

  def terminateExecutable(program: Executable): Executable = {
    program match {
      case matrix: Matrix => WriteMatrix(matrix)
      case scalar: ScalarRef => WriteScalar(scalar)
      case string: StringRef => WriteString(string)
      case function: FunctionRef => WriteFunction(function)
      case _ => program
    }
  }
}
