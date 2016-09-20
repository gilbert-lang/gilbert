package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime.{withBreeze, EngineConfiguration, withFlink}
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

object CellArray {

  def main(args:Array[String]){
    val executable = Gilbert.compileRessource("cellArray.gb")

    val engineConfig = EngineConfiguration(parallelism = 4)
    val runtimeConfig = RuntimeConfiguration()

    withBreeze()
    withFlink.local(engineConfig).execute(executable, runtimeConfig)
  }
}