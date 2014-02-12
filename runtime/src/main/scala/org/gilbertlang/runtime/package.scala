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
import scala.language.implicitConversions

package object runtime {
  
  import Executables.{scalar, Matrix, FunctionRef, string, ScalarRef, function, MatrixParameter}
  
  implicit def Int2Scalar(value: Int) = scalar(value.toDouble)
  implicit def Double2Scalar(value: Double) = scalar(value)
  implicit def String2StringRef(value: String) = string(value)

  implicit def MatrixMatrix2FunctionRef(funct: Matrix => Matrix): FunctionRef = {
    function(1, funct(MatrixParameter(0)))
  }

  def scalarRef2Int(value: ScalarRef) = {
    value match {
      case scalar(value) => Some(value.toInt)
      case _ => None
    }
  }
}
