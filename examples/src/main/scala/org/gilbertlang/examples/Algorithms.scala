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

package org.gilbertlang.examples

import org.gilbertlang.runtime._
import org.gilbertlang.runtime.GilbertFunctions.{load, norm2, fixpoint}
import org.gilbertlang.runtime.spones
import org.gilbertlang.runtime.ones
import org.gilbertlang.runtime.randn
import org.gilbertlang.runtime.sum

object Algorithms {


  def powerIteration() = {

    val size = 10

    val A = randn(size, size, 0, 1)
    val x_0 = ones(size, 1) / size

    fixpoint(x_0, { x => {
      val nextX = A * x
      nextX / norm2(nextX)
    }})
  }

  def pageRank() = {

    val numVertices = 10

    val N = load("/home/ssc/Entwicklung/projects/gilbert/examples/src/main/resources/network.csv", numVertices,
                 numVertices)
    //val N = sprand(numVertices, numVertices, 0.25)

    /* create the adjacency matrix */
    val A = spones(N)

    /* outdegree per vertex */
    val d = sum(A, 2)

    /* create the column-stochastic transition matrix */
    val T = (diag(1 / d) * A).t

    /*  initialize the ranks */
    val r_0 = ones(numVertices, 1) / numVertices

    /* compute PageRank */
    val e = ones(numVertices, 1)

    fixpoint(r_0, { r => .85 * T * r + .15 * e })
  }



}