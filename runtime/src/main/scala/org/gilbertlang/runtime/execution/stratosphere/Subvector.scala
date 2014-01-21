package org.gilbertlang.runtime.execution.stratosphere

import eu.stratosphere.types.Value
import org.apache.mahout.math.Vector
import java.io.DataOutput
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import scala.collection.convert.WrapAsScala
import java.io.IOException
import java.io.DataInput
import org.apache.mahout.math.function.DoubleFunction
import org.apache.mahout.math.function.DoubleDoubleFunction
import org.apache.mahout.math.OrderedIntDoubleMapping

case class Subvector(var vector: VectorValue, var index: Int, var offset: Int, var numTotalEntries: Int) extends Vector {

  def this() = this(null, -1, -1, -1)

  /** @return a formatted String suitable for output */
  def asFormatString() = vector.asFormatString()

  /**
   * Assign the value to all elements of the receiver
   *
   * @param value a double value
   * @return the modified receiver
   */
  def assign(value: Double) = {
    vector.assign(value)
    this
  }

  /**
   * Assign the values to the receiver
   *
   * @param values a double[] of values
   * @return the modified receiver
   * @throws CardinalityException if the cardinalities differ
   */
  def assign(values: Array[Double]) = {
    vector.assign(values)
    this
  }

  /**
   * Assign the other vector values to the receiver
   *
   * @param other a Vector
   * @return the modified receiver
   * @throws CardinalityException if the cardinalities differ
   */
  def assign(other: Vector) = {
    vector.assign(other)
    this
  }

  /**
   * Apply the function to each element of the receiver
   *
   * @param function a DoubleFunction to apply
   * @return the modified receiver
   */
  def assign(function: DoubleFunction) = {
    vector.assign(function)
    this
  }

  /**
   * Apply the function to each element of the receiver and the corresponding element of the other argument
   *
   * @param other    a Vector containing the second arguments to the function
   * @param function a DoubleDoubleFunction to apply
   * @return the modified receiver
   * @throws CardinalityException if the cardinalities differ
   */
  def assign(other: Vector, function: DoubleDoubleFunction) = {
    vector.assign(other, function)
    this
  }

  /**
   * Apply the function to each element of the receiver, using the y value as the second argument of the
   * DoubleDoubleFunction
   *
   * @param f a DoubleDoubleFunction to be applied
   * @param y a double value to be argument to the function
   * @return the modified receiver
   */
  def assign(f: DoubleDoubleFunction, y: Double) = {
    vector.assign(f, y)
    this
  }

  /**
   * Return the cardinality of the recipient (the maximum number of values)
   *
   * @return an int
   */
  def size = {
    vector.size()
  }

  /**
   * @return true iff this implementation should be considered dense -- that it explicitly
   *  represents every value
   */
  def isDense: Boolean = {
    vector.isDense()
  }

  /**
   * @return true iff this implementation should be considered to be iterable in index order in an efficient way.
   *  In particular this implies that {@link #all()} and {@link #nonZeroes()} ()} return elements
   *  in ascending order by index.
   */
  def isSequentialAccess(): Boolean = {
    vector.isSequentialAccess()
  }

  /**
   * Return a copy of the recipient
   *
   * @return a new Vector
   */
  override def clone() = {
    Subvector(Helper.clone(vector), index, offset, numTotalEntries)
  }

  def all() = vector.all()

  def nonZeroes() = vector.nonZeroes()

  /**
   * Return an object of Vector.Element representing an element of this Vector. Useful when designing new iterator
   * types.
   *
   * @param index Index of the Vector.Element required
   * @return The Vector.Element Object
   */
  def getElement(index: Int) = vector.getElement(index)

  /**
   * Merge a set of (index, value) pairs into the vector.
   * @param updates an ordered mapping of indices to values to be merged in.
   */
  def mergeUpdates(updates: OrderedIntDoubleMapping) {
    vector.mergeUpdates(updates)
  }

  /**
   * Return a new vector containing the values of the recipient divided by the argument
   *
   * @param x a double value
   * @return a new Vector
   */
  def divide(x: Double) = {
    Subvector(vector.divide(x), index, offset, numTotalEntries)
  }

  /**
   * Return the dot product of the recipient and the argument
   *
   * @param x a Vector
   * @return a new Vector
   * @throws CardinalityException if the cardinalities differ
   */
  def dot(x: Vector) = {
    vector.dot(x)
  }

  /**
   * Return the value at the given index
   *
   * @param index an int index
   * @return the double at the index
   * @throws IndexException if the index is out of bounds
   */
  def get(index: Int) = vector.get(index)

  /**
   * Return the value at the given index, without checking bounds
   *
   * @param index an int index
   * @return the double at the index
   */
  def getQuick(index: Int) = vector.getQuick(index)

  /**
   * Return an empty vector of the same underlying class as the receiver
   *
   * @return a Vector
   */
  def like() = Subvector(vector.like(), index, offset, numTotalEntries)

  /**
   * Return a new vector containing the element by element difference of the recipient and the argument
   *
   * @param x a Vector
   * @return a new Vector
   * @throws CardinalityException if the cardinalities differ
   */
  def minus(x: Vector) = Subvector(vector.minus(x), index, offset, numTotalEntries)

  /**
   * Return a new vector containing the normalized (L_2 norm) values of the recipient
   *
   * @return a new Vector
   */
  def normalize() = Subvector(vector.normalize(), index, offset, numTotalEntries)

  /**
   * Return a new Vector containing the normalized (L_power norm) values of the recipient. <p/> See
   * http://en.wikipedia.org/wiki/Lp_space <p/> Technically, when 0 < power < 1, we don't have a norm, just a metric,
   * but we'll overload this here. <p/> Also supports power == 0 (number of non-zero elements) and power = {@link
   * Double#POSITIVE_INFINITY} (max element). Again, see the Wikipedia page for more info
   *
   * @param power The power to use. Must be >= 0. May also be {@link Double#POSITIVE_INFINITY}. See the Wikipedia link
   *              for more on this.
   * @return a new Vector x such that norm(x, power) == 1
   */
  def normalize(power: Double) = Subvector(vector.normalize(power), index, offset, numTotalEntries)

  /**
   * Return a new vector containing the log(1 + entry)/ L_2 norm  values of the recipient
   *
   * @return a new Vector
   */
  def logNormalize() = Subvector(vector.logNormalize, index, offset, numTotalEntries)

  /**
   * Return a new Vector with a normalized value calculated as log_power(1 + entry)/ L_power norm. <p/>
   *
   * @param power The power to use. Must be > 1. Cannot be {@link Double#POSITIVE_INFINITY}.
   * @return a new Vector
   */
  def logNormalize(power: Double) = Subvector(vector.logNormalize(power), index, offset, numTotalEntries)

  /**
   * Return the k-norm of the vector. <p/> See http://en.wikipedia.org/wiki/Lp_space <p/> Technically, when 0 &gt; power
   * &lt; 1, we don't have a norm, just a metric, but we'll overload this here. Also supports power == 0 (number of
   * non-zero elements) and power = {@link Double#POSITIVE_INFINITY} (max element). Again, see the Wikipedia page for
   * more info.
   *
   * @param power The power to use.
   * @see #normalize(double)
   */
  def norm(power: Double) = vector.norm(power)

  /** @return The minimum value in the Vector */
  def minValue = vector.minValue()

  /** @return The index of the minimum value */
  def minValueIndex = vector.minValueIndex()

  /** @return The maximum value in the Vector */
  def maxValue = vector.maxValue()

  /** @return The index of the maximum value */
  def maxValueIndex = vector.maxValueIndex()

  /**
   * Return a new vector containing the sum of each value of the recipient and the argument
   *
   * @param x a double
   * @return a new Vector
   */
  def plus(x: Double) = Subvector(vector.plus(x), index, offset, numTotalEntries)

  /**
   * Return a new vector containing the element by element sum of the recipient and the argument
   *
   * @param x a Vector
   * @return a new Vector
   * @throws CardinalityException if the cardinalities differ
   */
  def plus(x: Vector) = Subvector(vector.plus(x), index, offset, numTotalEntries)

  /**
   * Set the value at the given index
   *
   * @param index an int index into the receiver
   * @param value a double value to set
   * @throws IndexException if the index is out of bounds
   */
  def set(index: Int, value: Double) {
    vector.set(index, value)
  }

  /**
   * Set the value at the given index, without checking bounds
   *
   * @param index an int index into the receiver
   * @param value a double value to set
   */
  def setQuick(index: Int, value: Double) {
    vector.setQuick(index, value)
  }

  /**
   * Increment the value at the given index by the given value.
   *
   * @param index an int index into the receiver
   * @param increment sets the value at the given index to value + increment;
   */
  def incrementQuick(index: Int, increment: Double) {
    vector.incrementQuick(index, increment)
  }

  /**
   * Return the number of values in the recipient which are not the default value.  For instance, for a
   * sparse vector, this would be the number of non-zero values.
   *
   * @return an int
   */
  def getNumNondefaultElements = vector.getNumNondefaultElements()

  /**
   * Return the number of non zero elements in the vector.
   *
   * @return an int
   */
  def getNumNonZeroElements = vector.getNumNonZeroElements()

  /**
   * Return a new vector containing the product of each value of the recipient and the argument
   *
   * @param x a double argument
   * @return a new Vector
   */
  def times(x: Double) = Subvector(vector.times(x), index, offset, numTotalEntries)

  /**
   * Return a new vector containing the element-wise product of the recipient and the argument
   *
   * @param x a Vector argument
   * @return a new Vector
   * @throws CardinalityException if the cardinalities differ
   */
  def times(x: Vector) = Subvector(vector.times(x), index, offset, numTotalEntries)

  /**
   * Return a new vector containing the subset of the recipient
   *
   * @param offset an int offset into the receiver
   * @param length the cardinality of the desired result
   * @return a new Vector
   * @throws CardinalityException if the length is greater than the cardinality of the receiver
   * @throws IndexException       if the offset is negative or the offset+length is outside of the receiver
   */
  def viewPart(offset: Int, length: Int) = Subvector(vector.viewPart(offset, length), index, offset + this.offset,
      numTotalEntries)

  /**
   * Return the sum of all the elements of the receiver
   *
   * @return a double
   */
  def zSum = vector.zSum()

  /**
   * Return the cross product of the receiver and the other vector
   *
   * @param other another Vector
   * @return a Matrix
   */
  def cross(other: Vector) = {
    vector.cross(other)
  }

  def cross(subvector: Subvector) = {
    Submatrix(vector.cross(subvector), index, subvector.index, offset, subvector.offset, numTotalEntries, subvector.numTotalEntries)
  }

  /*
   * Need stories for these but keeping them here for now.
   */
  // void getNonZeros(IntArrayList jx, DoubleArrayList values);
  // void foreachNonZero(IntDoubleFunction f);
  // DoubleDoubleFunction map);
  // NewVector assign(Vector y, DoubleDoubleFunction function, IntArrayList
  // nonZeroIndexes);

  /**
   * Examples speak louder than words:  aggregate(plus, pow(2)) is another way to say
   * getLengthSquared(), aggregate(max, abs) is norm(Double.POSITIVE_INFINITY).  To sum all of the postive values,
   * aggregate(plus, max(0)).
   * @param aggregator used to combine the current value of the aggregation with the result of map.apply(nextValue)
   * @param map a function to apply to each element of the vector in turn before passing to the aggregator
   * @return the final aggregation
   */
  def aggregate(aggregator: DoubleDoubleFunction, map: DoubleFunction) = {
    vector.aggregate(aggregator, map)
  }

  /**
   * }
   * <p>Generalized inner product - take two vectors, iterate over them both, using the combiner to combine together
   * (and possibly map in some way) each pair of values, which are then aggregated with the previous accumulated
   * value in the combiner.</p>
   * <p>
   * Example: dot(other) could be expressed as aggregate(other, Plus, Times), and kernelized inner products (which
   * are symmetric on the indices) work similarly.
   * @param other a vector to aggregate in combination with
   * @param aggregator function we're aggregating with; fa
   * @param combiner function we're combining with; fc
   * @return the final aggregation; if r0 = fc(this[0], other[0]), ri = fa(r_{i-1}, fc(this[i], other[i]))
   * for all i > 0
   */
  def aggregate(other: Vector, aggregator: DoubleDoubleFunction, combiner: DoubleDoubleFunction) = {
    vector.aggregate(other, aggregator, combiner)
  }

  /** Return the sum of squares of all elements in the vector. Square root of this value is the length of the vector. */
  def getLengthSquared = vector.getLengthSquared()

  /** Get the square of the distance between this vector and the other vector. */
  def getDistanceSquared(v: Vector) = vector.getDistanceSquared(v)

  /**
   * Gets an estimate of the cost (in number of operations) it takes to lookup a random element in this vector.
   */
  def getLookupCost = vector.getLookupCost()

  /**
   * Gets an estimate of the cost (in number of operations) it takes to advance an iterator through the nonzero
   * elements of this vector.
   */
  def getIteratorAdvanceCost = vector.getIteratorAdvanceCost()

  /**
   * Return true iff adding a new (nonzero) element takes constant time for this vector.
   */
  def isAddConstantTime = vector.isAddConstantTime()
}