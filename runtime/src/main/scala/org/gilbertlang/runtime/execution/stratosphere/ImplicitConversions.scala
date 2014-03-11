package org.gilbertlang.runtime.execution.stratosphere

import eu.stratosphere.api.scala.DataSet
import eu.stratosphere.api.common.operators.Operator
import scala.reflect.ClassTag
import eu.stratosphere.api.common.operators.{CollectionDataSource => JavaCollectionDataSource }
import eu.stratosphere.api.common.io.{CollectionInputFormat => JavaCollectionInputFormat}
import eu.stratosphere.core.io.GenericInputSplit
import eu.stratosphere.types.{Value, Record}
import scala.language.implicitConversions

/**
 * Created by till on 10/03/14.
 */
object ImplicitConversions {

  implicit def dataset2Operator[T](dataset: DataSet[T]): Operator = dataset.contract
  implicit def dataset2ValueExtractor[T](dataset: DataSet[T]): ValueExtractor[T] = new ValueExtractor(dataset)

  class ValueExtractor[T](dataset: DataSet[T]){
    def getValue[U <: Value : ClassTag](index: Int, fieldNum: Int): U = {
      if(dataset.contract.isInstanceOf[JavaCollectionDataSource]){
        val collectionDataSource = dataset.asInstanceOf[JavaCollectionDataSource]
        val inputFormat = collectionDataSource.getFormatWrapper.getUserCodeObject
          .asInstanceOf[JavaCollectionInputFormat]
        val record = new Record()
        inputFormat.open(new GenericInputSplit())

        for(counter <- 1 until index){
          inputFormat.nextRecord(record)
        }
        val classTag = implicitly[ClassTag[U]]

        record.getField[U](fieldNum, classTag.runtimeClass.asInstanceOf[Class[U]])
      }else{
        throw new IllegalArgumentException("Dataset has to be of type CollectionDataSource and not " +
          dataset.getClass)
      }
    }
  }


}
