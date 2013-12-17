/**
 * gilbert - Distributed Linear Algebra on Sparse Matrices
 * Copyright (C) 2013  Sebastian Schelter
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

object GilbertFunctions {
  object load {
    def apply(path: StringRef, numRows: ScalarRef, numColumns: ScalarRef) = LoadMatrix(path, numRows, numColumns)
  }

  object fixpoint {
    def apply(initialState: Matrix, updateFunction: Matrix => Matrix) = {
      new FixpointIteration(initialState, updateFunction)
    }
  }

  object binarize {
    def apply(matrix: Matrix) = matrix.binarize()
  }

  object max {
    def apply(matrix: Matrix) = matrix.max()
  }

  object norm2 {
    def apply(matrix: Matrix) = matrix.norm(2)
  }
}

