package org.gilbertlang.examples

import org.gilbertlang.runtime.withStratosphere
import eu.stratosphere.client.LocalExecutor
import org.gilbertlang.language.Gilbert

object StratosphereExecutor {

  def main(args: Array[String]) {
    val executable = Gilbert.compileRessource("stratosphere.gb")

    val plan = withStratosphere(executable)
    LocalExecutor.execute(plan)
  }
}