package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import eu.stratosphere.client.LocalExecutor
import org.gilbertlang.runtime._
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

object Kmeans {

  def main(args:Array[String]){
    val executable = Gilbert.compileRessource("kmeans.gb")
    val engineConfiguration = EngineConfiguration()
    val runtimeConfig = RuntimeConfiguration()

    withBreeze()
    withSpark.local(engineConfiguration).execute(executable, runtimeConfig)
  }
}