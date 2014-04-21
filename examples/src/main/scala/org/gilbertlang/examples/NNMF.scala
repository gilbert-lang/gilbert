package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime.{withSpark, local, withStratosphere}
import eu.stratosphere.client.{RemoteExecutor, LocalExecutor}
import scala.collection.JavaConverters._
import java.util.logging.{ConsoleHandler, Level, Logger}

object NNMF {

  def main(args:Array[String]){
    val logger = Logger.getLogger("com.github.fommil.jni.JniLoader");
    val handler = new ConsoleHandler();
    handler.setLevel(Level.FINEST);
    logger.addHandler(handler)
    logger.setLevel(Level.FINEST);

    val executable = Gilbert.compileRessource("nnmf.gb")

    val plan = withStratosphere(executable)

    val jarFiles = List("runtime/target/runtime-0.1-SNAPSHOT.jar", "runtimeMacros/target/runtimeMacros-0.1-SNAPSHOT" +
      ".jar", "/Users/till/.m2/repository/org/scalanlp/breeze_2.10/0.6-SNAPSHOT/breeze_2.10-0.6-SNAPSHOT.jar",
      "/Users/till/.m2/repository/com/github/fommil/netlib/native_ref-java/1.1/native_ref-java-1.1.jar",
      "/Users/till/.m2/repository/com/github/fommil/netlib/core/1.1.1/core-1.1.1.jar",
      "/Users/till/.m2/repository/",
      "/Users/till/.m2/repository/com/github/fommil/jniloader/1.1/jniloader-1.1.jar");
    val executor = new RemoteExecutor("node1.stsffap.org", 6123, jarFiles.asJava);

    executor.executePlan(plan)

//    LocalExecutor.execute(plan)
//    withSpark(executable)
//    local(executable)
  }
}