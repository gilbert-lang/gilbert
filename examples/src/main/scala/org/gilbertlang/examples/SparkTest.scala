package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime.withSpark

object SparkTest {

  def main(args:Array[String]){
    val executable = Gilbert.compileRessource("testSpark.gb")

    withSpark(executable)
  }
}