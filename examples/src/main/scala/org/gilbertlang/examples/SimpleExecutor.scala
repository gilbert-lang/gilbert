package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import eu.stratosphere.client.LocalExecutor
import org.gilbertlang.runtime.{withSpark, local, withStratosphere}

object SimpleExecutor {

  def main(args:Array[String]){
    val executable = Gilbert.compileRessource("pagerank.gb")
    val optimized = Gilbert.optimize(executable, transposePushdown = true, mmReorder = true)

//    local(optimized)
    withSpark(optimized).local(1)
//    withStratosphere(optimized).local(4)
  }
}