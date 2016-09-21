package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime._
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

object Pagerank {
  def main(args: Array[String]) {
    val dop = 4;
    val path = "hdfs://node1.stsffap.org:54310/user/hduser/pagerank"

    val executable = Gilbert.compileRessource("pagerank.gb")

//    val runtimeConfiguration = RuntimeConfiguration(blocksize = 5, outputPath = Some(path),
//      compilerHints = true, verboseWrite = true)
    val runtimeConfiguration = RuntimeConfiguration(blocksize = 5, outputPath = None,
      compilerHints = true, verboseWrite = true)
    val sparkConfiguration = EngineConfiguration(appName = "Pagerank",master = "stsffap", port = 7077,
      parallelism = dop, libraryPath =
        "/Users/till/uni/ws14/dima/mastersthesis/workspace/gilbert/evaluation/target/lib/")
    val flinkConfiguration = EngineConfiguration(appName = "Pagerank", master = "localhost", port = 6123,
      parallelism = dop, libraryPath = "/Users/till/uni/ws14/dima/mastersthesis/workspace/gilbert/evaluation/target/lib/")

    withBreeze()
//    val result = withFlink.remote(flinkConfiguration).execute(executable, runtimeConfiguration)
//    val result = withFlink.local(flinkConfiguration).execute(executable, runtimeConfiguration)
    val result = withSpark.remote(sparkConfiguration).execute(executable, runtimeConfiguration.copy(outputPath = None))
//    val result = withSpark.local(sparkConfiguration).execute(executable, runtimeConfiguration)
//    val result = local().execute(executable, runtimeConfiguration)

    println(result)
  }
}