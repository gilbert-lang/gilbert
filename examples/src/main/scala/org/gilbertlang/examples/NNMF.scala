package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime.{local, withStratosphere}
import eu.stratosphere.client.LocalExecutor

object NNMF {

  def main(args:Array[String]){
    val executable = Gilbert.compileRessource("nnmf.gb")

    val plan = withStratosphere(executable)
    LocalExecutor.execute(plan)
//    local(executable)
  }
}