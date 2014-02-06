package org.gilbertlang.examples

import org.apache.commons.io.IOUtils
import java.io.InputStreamReader
import java.io.Reader
import org.gilbertlang.language.Gilbert
import org.gilbertlang.runtime.withStratosphere
import eu.stratosphere.client.LocalExecutor
import java.io.DataOutputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import eu.stratosphere.api.scala.DataSource
import eu.stratosphere.api.scala.operators.CsvInputFormat
import eu.stratosphere.api.scala.operators.CsvOutputFormat
import eu.stratosphere.api.scala.ScalaPlan

object ExecutorTest {

  def main(args: Array[String]) {
    val executable = Gilbert.compileRessource("stratosphere.gb")

        val plan = withStratosphere(executable)
        LocalExecutor.execute(plan)

//    val source = DataSource("file:///Users/till/uni/ws14/dima/mastersthesis/workspace/gilbert/examples/matrix.csv", CsvInputFormat[(Int, Int, Double)]("\n", ' '))
//
//    source map { x =>
//      val partition = Partition(-1, 0, 0, 1, 1, 0, 0, 1, 1)
//      val matrix = Submatrix(partition, 1)
//      matrix.setQuick(x._1, x._2, x._3)
//      matrix
//    }
//    
//    val sink = source.write("file:///Users/till/uni/ws14/dima/mastersthesis/workspace/gilbert/examples/output.csv", CsvOutputFormat())
//    
//    LocalExecutor.execute(new ScalaPlan(Seq(sink)))
  }

}