package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime.withSpark
import org.apache.log4j.{Level, Logger}

object SparkTest {

  def main(args:Array[String]){
    val tries = 1
    val executable1 = Gilbert.compileRessource("nnmf.gb")

    val start1 = System.nanoTime()
    for(counter <- 0 until tries)
      withSpark(executable1)
    val end1 = System.nanoTime()

    val n1 = 10
    println()
    println((end1-start1).toDouble/1000000000/n1/tries)
  }
}