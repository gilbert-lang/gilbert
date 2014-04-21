package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime.withSpark
import org.apache.log4j.{Level, Logger}

object SparkTest {

  def main(args:Array[String]){
    val executable = Gilbert.compileRessource("nnmf.gb")
    withSpark(executable).remote("spark://node1:7077", "Gilbert")
  }
}