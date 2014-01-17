package org.gilbertlang.runtime.execution.stratosphere

import eu.stratosphere.api.scala.OutputHintable
import eu.stratosphere.api.common.operators.GenericDataSource
import org.gilbertlang.runtimeMacros.io.LiteralInputFormat
import eu.stratosphere.api.scala.DataSet
import eu.stratosphere.api.scala.ScalaOperator

object LiteralDataSource {
  
  private val DEFAULT_NAME = "<Unnamed literal data source>"
    
  def apply[Out](literalValue: Out, format: LiteralInputFormat[Out]): DataSet[Out] with OutputHintable[Out] = {
    val result = new LiteralDataSource[Out]( format, List(literalValue))
    new DataSet(result) with OutputHintable[Out] {}
  }
  
  def apply[Out](literalValues: Iterable[Out], format: LiteralInputFormat[Out]): DataSet[Out] with OutputHintable[Out] = {
    val result =new LiteralDataSource[Out](format,literalValues)
    new DataSet(result) with OutputHintable[Out] {}
  }
}

class LiteralDataSource[Out](val format: LiteralInputFormat[Out], val values: Iterable[Out], name: String) 
extends GenericDataSource[LiteralInputFormat[Out]](format, name) with ScalaOperator[Out] {
  
  format.setData(values)
  
  def this(format: LiteralInputFormat[Out], values: Iterable[Out]) = this(format,values, LiteralDataSource.DEFAULT_NAME)
    
  override def getUDF = format.getUDF
  override def persistConfiguration() = format.persistConfiguration(this.getParameters())
}