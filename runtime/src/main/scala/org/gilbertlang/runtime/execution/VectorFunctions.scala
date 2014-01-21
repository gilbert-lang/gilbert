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

package org.gilbertlang.runtime.execution

import org.apache.mahout.math.function.{DoubleDoubleFunction, VectorFunction, DoubleFunction}
import org.apache.mahout.math.Vector

object VectorFunctions {

  def binarize = new DoubleFunction {
    def apply(value: Double) = { if (value == 0) { 0 } else { 1 } }
  }

  def sum = new VectorFunction {
    def apply(v: Vector) = v.zSum()
  }

  def lengthSquared = new VectorFunction {
    def apply(v: Vector) = v.getLengthSquared()
  }

  def max = new DoubleDoubleFunction {
    def apply(value1: Double, value2: Double) = math.max(value1, value2)
  }
  
  def maxVector = new VectorFunction{
    def apply(vector: Vector) = vector.maxValue()
  }
  
  def min = new DoubleDoubleFunction {
    def apply(a: Double, b: Double) = math.min(a,b)
  }
  
  def minVector = new VectorFunction{
    def apply(vector: Vector) = vector.minValue()
  }

  def identity = new DoubleFunction {
    def apply(value: Double) = value
  }
  
  def l1Norm = new VectorFunction {
    def apply(vector: Vector) = vector.norm(1)
  }
}
