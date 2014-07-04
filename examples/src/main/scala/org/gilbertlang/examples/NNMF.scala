package org.gilbertlang.examples

import java.io.{OutputStream}

import org.apache.log4j._
import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime._

import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

import scala.util.matching.Regex

object NNMF {

  def main(args:Array[String]){
    val dop = 4;
    val path = "hdfs://node1.stsffap.org:54310/user/hduser/"

    val executable = Gilbert.compileRessource("nnmf.gb")

    val jarFiles = List("runtime/target/runtime-0.1-SNAPSHOT.jar", "runtimeMacros/target/runtimeMacros-0.1-SNAPSHOT" +
      ".jar", "/Users/till/.m2/repository/org/scalanlp/breeze_2.10/0.8.1/breeze_2.10-0.8.1.jar",
      "/Users/till/.m2/repository/com/github/fommil/netlib/native_ref-java/1.1/native_ref-java-1.1.jar",
      "/Users/till/.m2/repository/com/github/fommil/netlib/core/1.1.2/core-1.1.2.jar",
      "/Users/till/.m2/repository/com/github/fommil/jniloader/1.1/jniloader-1.1.jar",
      "/Users/till/.m2/repository/com/github/fommil/netlib/netlib-native_system-linux-x86_64/1" +
        ".1/netlib-native_system-linux-x86_64-1.1-natives.jar",
      "/Users/till/.m2/repository/com/github/fommil/netlib/netlib-native_ref-linux-x86_64/1" +
        ".1/netlib-native_ref-linux-x86_64-1.1-natives.jar",
      "/Users/till/.m2/repository/org/apache/mahout/mahout-math/1.0-SNAPSHOT/mahout-math-1.0-SNAPSHOT.jar",
      "/Users/till/.m2/repository/eu/stratosphere/stratosphere-core/0.6-patched/stratosphere-core-0.6-patched.jar",
      "/Users/till/.m2/repository/org/apache/commons/commons-math3/3.3/commons-math3-3.3.jar");

    val runtimeConfiguration = RuntimeConfiguration(blocksize =  5, outputPath = Some(path))
    val sparkConfiguration = EngineConfiguration(appName = "NNMF",master = "node1", port = 7077, jars = jarFiles,
      parallelism = dop)
    val stratosphereConfiguration = EngineConfiguration(appName = "NNMF", master = "node1", port = 6123,
      parallelism = dop,
      jars =jarFiles)

    withBreeze()
//    val result = withStratosphere.remote(stratosphereConfiguration).execute(executable, runtimeConfiguration)


    val result = withSpark.remote(sparkConfiguration).execute(executable, runtimeConfiguration)

    println(result)

  }
}



