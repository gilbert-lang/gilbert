package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime.withSpark
import org.apache.log4j.{Level, Logger}

object SparkTest {

  def main(args:Array[String]){
    Logger.getRootLogger.setLevel(Level.OFF)
    val executable = Gilbert.compileRessource("testSpark.gb")

    withSpark(executable)
  }
}