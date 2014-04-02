package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import eu.stratosphere.client.LocalExecutor
import org.gilbertlang.runtime.{local, withStratosphere}

object SimpleExecutor {

  def main(args:Array[String]){
    val executable = Gilbert.compileRessource("builtin.gb")

//    val plan = withStratosphere(executable)
//    LocalExecutor.execute(plan)
    val result = local(executable)
    println(result)
  }
}