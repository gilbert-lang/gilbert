package org.gilbertlang.examples

import org.gilbertlang.runtime.{EngineConfiguration, withBreeze, withFlink}
import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

object FlinkExecutor {

  def main(args: Array[String]) {
    val executable = Gilbert.compileRessource("test.gb")

    val runtimeConfig = RuntimeConfiguration(blocksize = 100)
    val engineConfiguration = EngineConfiguration(parallelism = 4)

    withBreeze()
    withFlink.local(engineConfiguration).execute(executable, runtimeConfig)
  }
}