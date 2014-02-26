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

object StratosphereExecutor {

  def main(args: Array[String]) {
    val executable = Gilbert.compileRessource("stratosphere.gb")

    val plan = withStratosphere(executable)
    LocalExecutor.execute(plan)
  }
}