package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import eu.stratosphere.client.LocalExecutor
import org.gilbertlang.runtime.{withBreeze, EngineConfiguration, withStratosphere}
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

object NNMFStep {

  def main(args:Array[String]){
    val executable = Gilbert.compileRessource("NNMFStep.gb")
    val runtimeConfig = RuntimeConfiguration()
    val engineConfiguration = EngineConfiguration(parallelism = 4)

    withBreeze()
    withStratosphere.local(engineConfiguration).execute(executable, runtimeConfig)
  }
}