package org.gilbertlang.runtimeMacros.linalg

case class Subvector(val vectorValue: DoubleVectorValue, index: Int, offset: Int, totalEntries: Int) {

  def vector: DoubleVector = vectorValue.vector

  def copy: Subvector = Subvector(vector.copy, index, offset, totalEntries)

  def iterator: Iterator[(Int, Double)] = vector.iterator map { case (key, value) => (key + offset, value)}

  def asMatrix: Submatrix = {
    Submatrix(vector.asMatrix, index, 0, offset, 0, totalEntries, 1)
  }

  def +(subvector: Subvector) = {
    Subvector(vector + subvector.vector, index, offset, totalEntries)
  }

  def :/=(op: Double): Subvector = {
    Subvector(vector :/= op, index, offset, totalEntries)
  }

  def :/=(op: Subvector): Subvector = {
    Subvector(vector :/= op.vector, index, offset, totalEntries)
  }
}
