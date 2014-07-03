package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime._
import eu.stratosphere.client.LocalExecutor

object PagerankExecutor {
  def main(args: Array[String]) {
    val executable = Gilbert.compileRessource("kmeans.gb")

//    withBreeze()
    withMahout()
//    local(executable)
    
    withStratosphere(executable).local(4)
//    withSpark(executable).local(4)
  }
}