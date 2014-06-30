package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime.{withSpark, local, withStratosphere}

import java.util.logging.{ConsoleHandler, Level, Logger}

object NNMF {

  def main(args:Array[String]){
    val logger = Logger.getLogger("com.github.fommil.jni.JniLoader");
    val handler = new ConsoleHandler();
    val dop = 4;
    val path = "hdfs://node1.stsffap.org:54310/user/hduser/"
    handler.setLevel(Level.FINEST);
    logger.addHandler(handler)
    logger.setLevel(Level.FINEST);

    val executable = Gilbert.compileRessource("nnmf.gb")

    val jarFiles = List("runtime/target/runtime-0.1-SNAPSHOT.jar", "runtimeMacros/target/runtimeMacros-0.1-SNAPSHOT" +
      ".jar", "/Users/till/.m2/repository/org/scalanlp/breeze_2.10/0.6-SNAPSHOT/breeze_2.10-0.6-SNAPSHOT.jar",
      "/Users/till/.m2/repository/com/github/fommil/netlib/native_ref-java/1.1/native_ref-java-1.1.jar",
      "/Users/till/.m2/repository/com/github/fommil/netlib/core/1.1.1/core-1.1.1.jar",
      "/Users/till/.m2/repository/com/github/fommil/jniloader/1.1/jniloader-1.1.jar",
      "/Users/till/.m2/repository/com/github/fommil/netlib/netlib-native_system-linux-x86_64/1" +
        ".1/netlib-native_system-linux-x86_64-1.1-natives.jar",
      "/Users/till/.m2/repository/com/github/fommil/netlib/netlib-native_ref-linux-x86_64/1" +
        ".1/netlib-native_ref-linux-x86_64-1.1-natives.jar");

    //withStratosphere(executable).remote("node1", 6123, dop, outputPath, jarFiles)
    withSpark(executable).remote("spark://node1:7077",checkpointDir = "", appName="NNMF",parallelism=dop,
      outputPath = None,jars = jarFiles)
  }
}