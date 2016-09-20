package org.gilbertlang.examples

import org.gilbertlang.runtime.{EngineConfiguration, withFlink}
import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

object StratosphereExecutor {

  def main(args: Array[String]) {
    val executable = Gilbert.compileRessource("test.gb")

    val runtimeConfig = RuntimeConfiguration()
    val engineConfiguration = EngineConfiguration(parallelism = 4)
    withFlink.local(engineConfiguration).execute(executable, runtimeConfig)
  }
}