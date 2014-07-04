package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import eu.stratosphere.client.LocalExecutor
import org.gilbertlang.runtime.{EngineConfiguration, withStratosphere}
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

object TypeConversion {

  def main(args:Array[String]){
    val executable = Gilbert.compileRessource("typeConversion.gb")
    val runtimeConfig = RuntimeConfiguration()
    val engineConfiguration = EngineConfiguration(parallelism = 4)


    withStratosphere.local(engineConfiguration).execute(executable, runtimeConfig)
  }
}