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

import org.apache.mahout.math.{Matrix, MatrixWritable, VectorWritable, Vector}
import scala.language.implicitConversions

package object spark {

  implicit def v2Writable(v: Vector): VectorWritable = new VectorWritable(v)
  implicit def m2Writable(m: Matrix): MatrixWritable = new MatrixWritable(m)
  implicit def vw2v(vw: VectorWritable): Vector = vw.get()
  implicit def mw2m(mw: MatrixWritable): Matrix = mw.get()
}
