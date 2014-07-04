package org.gilbertlang.examples

import org.gilbertlang.runtime.{EngineConfiguration, local, withStratosphere}
import org.gilbertlang.language.Gilbert
import eu.stratosphere.client.LocalExecutor
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

object StratosphereExecutor {

  def main(args: Array[String]) {
    val executable = Gilbert.compileRessource("test.gb")

    val runtimeConfig = RuntimeConfiguration()
    val engineConfiguration = EngineConfiguration(parallelism = 4)
    withStratosphere.local(engineConfiguration).execute(executable, runtimeConfig)
  }
}