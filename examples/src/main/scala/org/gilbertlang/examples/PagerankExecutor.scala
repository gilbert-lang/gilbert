package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime._
import eu.stratosphere.client.LocalExecutor
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

object PagerankExecutor {
  def main(args: Array[String]) {
    val executable = Gilbert.compileRessource("kmeans.gb")
    val runtimeConfig = RuntimeConfiguration()
    val engineConfiguration = EngineConfiguration(parallelism = 4)

//    withBreeze()
    withMahout()
//    local(executable)
    
    withStratosphere.local(engineConfiguration).execute(executable, runtimeConfig)
//    withSpark.local(engineConfiguration).execute(executable, runtimeConfiguration)
  }
}