package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import eu.stratosphere.client.LocalExecutor
import org.gilbertlang.runtime.withStratosphere

object Kmeans {

  def main(args:Array[String]){
    val executable = Gilbert.compileRessource("kmeans.gb")

    val plan = withStratosphere(executable);
    LocalExecutor.execute(plan)
  }
}