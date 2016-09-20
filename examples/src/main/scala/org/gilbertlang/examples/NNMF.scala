package org.gilbertlang.examples

import java.io.{OutputStream}

import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime._

import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

object NNMF {

  def main(args:Array[String]){
    val dop = 3;
    val path = "hdfs://node1.stsffap.org:54310/user/hduser/nnmf"

    val executable = Gilbert.compileRessource("nnmf.gb")

    val runtimeConfiguration = RuntimeConfiguration(blocksize = 5, outputPath = Some(path),
      compilerHints = true, verboseWrite = true)
    val sparkConfiguration = EngineConfiguration(appName = "NNMF",master = "node1", port = 7077,
      parallelism = dop, libraryPath =
        "/Users/till/uni/ws14/dima/mastersthesis/workspace/gilbert/evaluation/target/lib/")
    val stratosphereConfiguration = EngineConfiguration(appName = "NNMF", master = "node1", port = 6123,
      parallelism = dop, libraryPath = "/Users/till/uni/ws14/dima/mastersthesis/workspace/gilbert/evaluation/target/lib/")

    withBreeze()
    val result = withFlink.remote(stratosphereConfiguration).execute(executable, runtimeConfiguration)
//    val result = withSpark.remote(sparkConfiguration).execute(executable, runtimeConfiguration.copy(outputPath = None))
//    val result = local().execute(executable, runtimeConfiguration)

    println(result)

  }
}



