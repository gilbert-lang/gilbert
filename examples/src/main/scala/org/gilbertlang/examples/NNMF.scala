package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime.{withSpark, local, withStratosphere}
import eu.stratosphere.client.LocalExecutor

object NNMF {

  def main(args:Array[String]){
    val executable = Gilbert.compileRessource("nnmf.gb")

//    val plan = withStratosphere(executable)
//    LocalExecutor.execute(plan)
    withSpark(executable)
//    local(executable)
  }
}