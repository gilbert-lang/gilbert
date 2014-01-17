package org.gilbertlang.runtimeMacros.io

import scala.reflect.macros.Context
import eu.stratosphere.api.scala.codegen.MacroContextHolder
import eu.stratosphere.api.scala.operators.ScalaInputFormatBase
import scala.language.experimental.macros
import eu.stratosphere.types.Record
import eu.stratosphere.api.common.io.InputFormat
import java.io.IOException
import eu.stratosphere.types.Value
import org.apache.commons.logging.LogFactory
import eu.stratosphere.api.common.io.UnsplittableInput
import eu.stratosphere.api.common.io.statistics.BaseStatistics
import eu.stratosphere.configuration.Configuration
import eu.stratosphere.api.scala.ScalaInputFormat
import eu.stratosphere.api.scala.analysis.UDT
import eu.stratosphere.api.scala.analysis.UDF0
import eu.stratosphere.api.scala.analysis.UDTSerializer
import eu.stratosphere.core.io.GenericInputSplit
import eu.stratosphere.api.java.record.io.GenericInputFormat

object LiteralInputFormat {

  def apply[Out](): LiteralInputFormat[Out] = macro impl[Out]

  def impl[Out: c.WeakTypeTag](c: Context)(): c.Expr[LiteralInputFormat[Out]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)

    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]

    val pact4sFormat = reify {
      new LiteralInputFormat[Out] {
        override val udt = c.Expr(createUdtOut).splice
      }
    }

    val result = c.Expr[LiteralInputFormat[Out]](Block(List(udtOut), pact4sFormat.tree))

    return result
  }
}

@SerialVersionUID(2L)
trait LiteralInputFormat[Out] extends GenericInputFormat with ScalaInputFormat[Out] {
  val LOG = LogFactory.getLog(this.getClass())

  private var data: Iterable[Out] = _
  private var startIndex: Int = -1
  private var endIndex: Int = -1
  protected var iterator: Iterator[Out] = null
  protected val udt: UDT[Out]
  lazy val udf: UDF0[Out] = new UDF0(udt)
  def getUDF: UDF0[Out] = udf
  protected var serializer: UDTSerializer[Out] = _
  protected var outputLength: Int = _

  override def configure(configuration: Configuration) {
    this.outputLength = udf.getOutputLength
    this.serializer = udf.getOutputSerializer
  }

  def setData(data: Iterable[Out]) {
    this.data = data
  }

  @throws(classOf[IOException])
  override def open(genericSplit: GenericInputSplit) {
    if (!genericSplit.isInstanceOf[LiteralInputSplit]) {
      throw new IOException("Input split has to of type LiteralInputSplit")
    }

    val split = genericSplit.asInstanceOf[LiteralInputSplit]

    if (split == null) {
      throw new IllegalArgumentException("Input split cannot be null")
    }
    if (split.startIndex < 0) {
      throw new IllegalArgumentException("Split start index cannot be less than 0")
    }
    if (split.startIndex > split.endIndex) {
      throw new IllegalArgumentException("Split start index cannot be greater than the end index")
    }
    if (split.endIndex > data.size) {
      throw new IllegalArgumentException("Split end index out of bounds")
    }

    if (endIndex != -1 && endIndex <= split.startIndex) {
      iterator.drop(split.startIndex - endIndex)
    } else {
      iterator = data.iterator
      iterator.drop(split.startIndex)
    }

    startIndex = split.startIndex
    endIndex = split.endIndex
  }

  override def close() {
    startIndex = -1
    endIndex = -1
    iterator = null
  }

  override def reachedEnd(): Boolean = {
    !iterator.hasNext
  }

  override def createInputSplits(numSplits: Int): Array[GenericInputSplit] = {
    if (numSplits < 1) {
      throw new IllegalArgumentException("Number input splits has to greater than 0")
    }

    val actualSplits = if (this.isInstanceOf[UnsplittableInput]) 1 else math.min(numSplits, data.size)
    val result = new Array[GenericInputSplit](actualSplits)
    val fraction = data.size.toDouble / actualSplits
    var counter = 0.0

    for (split <- 0 until actualSplits) {
      val startIndex = counter.toInt
      counter += fraction
      val endIndex = counter.toInt
      val newSplit = new LiteralInputSplit(split, startIndex, endIndex)
      result(split) = newSplit
    }

    result
  }

  @throws(classOf[IOException])
  override def getStatistics(cachedStatistics: BaseStatistics): BaseStatistics = {
    if (cachedStatistics == null) {
      LiteralBaseStatistics(0, data.size, 0)
    } else {
      cachedStatistics
    }
  }

  override def nextRecord(record: Record): Boolean = {
    record.setNumFields(outputLength)
    serializer.serialize(iterator.next(), record)
    true
  }

}