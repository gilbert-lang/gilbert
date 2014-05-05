package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime.{withSpark, local, withStratosphere}
import eu.stratosphere.client.LocalExecutor

object PagerankExecutor {
  def main(args: Array[String]) {
    val executable = Gilbert.compileRessource("pagerank.gb")
    
    withStratosphere(executable).local(4)
  }
}