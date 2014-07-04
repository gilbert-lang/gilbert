package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import eu.stratosphere.client.LocalExecutor
import org.gilbertlang.runtime.{withBreeze, EngineConfiguration, withStratosphere}
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

object CellArray {

  def main(args:Array[String]){
    val executable = Gilbert.compileRessource("cellArray.gb")

    val engineConfig = EngineConfiguration(parallelism = 4)
    val runtimeConfig = RuntimeConfiguration()

    withBreeze()
    withStratosphere.local(engineConfig).execute(executable, runtimeConfig)
  }
}