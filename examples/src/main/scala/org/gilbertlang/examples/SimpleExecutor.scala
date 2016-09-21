package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime._
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

object SimpleExecutor {

  def main(args:Array[String]){
//    val input = "Job 0 finished: saveAsTextFile at SparkExecutor.scala:263, took 1.328579 s";
//    val regex = """Job \d+ finished: [^,]*, took ([0-9\.]*) s""".r
//
//    val result = regex.findFirstMatchIn(input)
//    for(r <- result) {
//      println(r.group(1))
//    }

    val executable = Gilbert.compileRessource("test.gb")
    val optimized = Gilbert.optimize(executable, transposePushdown = true, mmReorder = true)

    val runtimeConfig =new RuntimeConfiguration(blocksize = 100,
      checkpointDir = Some("/Users/till/uni/ws14/dima/mastersthesis/workspace/gilbert/spark"),
      iterationsUntilCheckpoint = 3, outputPath = Some("/tmp/gilbert/"), verboseWrite = false)
    val engineConfiguration = EngineConfiguration(parallelism=1, master="node1", port=6123, libraryPath ="/Users/till/uni/ws14/dima/mastersthesis/workspace/gilbert/"  )

//    withMahout()
    withBreeze()
//    local().execute(optimized, runtimeConfig)
    val time = withSpark.local(engineConfiguration).execute(optimized, runtimeConfig)
//    withStratosphere.local(engineConfiguration).execute(optimized, runtimeConfig)

    println(time)
  }
}