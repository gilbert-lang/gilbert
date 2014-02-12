package org.gilbertlang.runtime.execution.stratosphere

import scala.util.Random

class GaussianRandom(val random: java.util.Random, val mean: Double =1, val std: Double = 1) extends Random(random) {
  def this() = this(new java.util.Random())
  def this(mean: Double, std: Double) = this(new java.util.Random(), mean, std)
  def this(seed: Long, mean: Double, std: Double) = this(new java.util.Random(seed), mean, std)
  def this(seed: Long) = this(new java.util.Random(seed))
  
  override def nextDouble(): Double = {
    nextGaussian*std + mean
  }
}