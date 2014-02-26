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
package execution

import org.apache.mahout.math.function.DoubleDoubleFunction
import org.apache.mahout.math.function.DoubleFunction

object CellwiseFunctions {

  def divide = new DoubleDoubleFunction{
    def apply(a: Double, b: Double) = { a / b }
  }
  
  def times = new DoubleDoubleFunction {
    def apply(a: Double, b: Double) = a * b
  }
  
  def max = new DoubleDoubleFunction {
    def apply(a: Double, b: Double) = math.max(a, b)
  }
  
  def min = new DoubleDoubleFunction {
    def apply(a: Double, b: Double) = math.min(a, b)
  }
  
  def binarize = new DoubleFunction {
    def apply(a: Double) = {
      if (a == 0) {
        0
      } else {
        1
      }
    }
  }
  
  def greaterThan = new DoubleDoubleFunction {
    def apply(a: Double, b: Double) = a > b
  }
  
  def greaterEqualThan = new DoubleDoubleFunction {
    def apply(a: Double, b: Double) = a >= b
  }
  
  def lessThan = new DoubleDoubleFunction {
    def apply(a: Double, b: Double) = a < b
  }
  
  def lessEqualThan = new DoubleDoubleFunction {
    def apply(a: Double, b: Double) = a <= b
  }
  
  def equals = new DoubleDoubleFunction {
    def apply(a: Double, b: Double) = a == b
  }
  
  def logicalAnd = new DoubleDoubleFunction {
    def apply(a: Double, b: Double) = a && b
  }
  
  def logicalOr = new DoubleDoubleFunction {
    def apply(a: Double, b: Double) = a || b
  }
}