package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime._
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

object SimpleExecutor {

  def main(args:Array[String]){
    val executable = Gilbert.compileRessource("test.gb")
    val optimized = Gilbert.optimize(executable, transposePushdown = true, mmReorder = true)

    val runtimeConfig =new RuntimeConfiguration(blocksize = 100,
      checkpointDir = Some("/Users/till/uni/ws14/dima/mastersthesis/workspace/gilbert/spark"),
      iterationsUntilCheckpoint = 3, outputPath = Some("/tmp/gilbert/"), verboseWrite = false)
    val engineConfiguration = EngineConfiguration(parallelism=1, master="node1", port=6123, libraryPath ="/Users/till/uni/ws14/dima/mastersthesis/workspace/gilbert/"  )

//    withMahout()
    withBreeze()
//    local().execute(optimized, runtimeConfig)
    withSpark.local(engineConfiguration).execute(optimized, runtimeConfig)
//    withStratosphere.local(engineConfiguration).execute(optimized, runtimeConfig)
  }
}