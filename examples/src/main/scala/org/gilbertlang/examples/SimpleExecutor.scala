package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import eu.stratosphere.client.LocalExecutor
import org.gilbertlang.runtime.{local, withStratosphere}

object SimpleExecutor {

  def main(args:Array[String]){
    val executable = Gilbert.compileRessource("optimization.gb")
    val optimized = Gilbert.optimize(executable, transposePushdown = true, mmReorder = true)

    withStratosphere(optimized).local(1)
  }
}