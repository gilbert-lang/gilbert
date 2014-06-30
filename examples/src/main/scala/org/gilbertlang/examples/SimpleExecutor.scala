package org.gilbertlang.examples

import org.gilbertlang.language.Gilbert
import eu.stratosphere.client.LocalExecutor
import org.gilbertlang.runtime._

object SimpleExecutor {

  def main(args:Array[String]){
    val executable = Gilbert.compileRessource("nnmf.gb")
    val optimized = Gilbert.optimize(executable, transposePushdown = true, mmReorder = true)


    withMahout()
//    local(optimized)
    withSpark(optimized).local(4, checkpointDir = "/Users/till/uni/ws14/dima/mastersthesis/workspace/gilbert/spark",
      iterationsUntilCheckpoint = 3)
//    withStratosphere(optimized).local(4)
  }
}