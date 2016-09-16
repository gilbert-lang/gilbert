package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import eu.stratosphere.client.LocalExecutor
import org.gilbertlang.runtime.{withSpark, withBreeze, EngineConfiguration, withStratosphere}
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

object NNMFStep {

  def main(args:Array[String]){
    val executable = Gilbert.compileRessource("NNMFStep.gb")
    val runtimeConfig = RuntimeConfiguration()
    val engineConfiguration = EngineConfiguration(parallelism = 4)

    withBreeze()
    withSpark.remote(engineConfiguration).execute(executable, runtimeConfig)
  }
}