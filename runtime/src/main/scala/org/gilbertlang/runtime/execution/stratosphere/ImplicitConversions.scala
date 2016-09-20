package org.gilbertlang.runtime.execution.stratosphere

import org.apache.flink.api.java.io.CollectionInputFormat
import org.apache.flink.api.java.operators.DataSource
import org.apache.flink.api.scala._
import org.apache.flink.core.io.GenericInputSplit

import scala.language.implicitConversions

/**
 * Created by till on 10/03/14.
 */
object ImplicitConversions {
  implicit def dataset2ValueExtractor[T](dataset: DataSet[T]): ValueExtractor[T] = new ValueExtractor(dataset)

  class ValueExtractor[T](dataset: DataSet[T]){
    def getValue(index: Int): T = {
      if (dataset.isInstanceOf[DataSource[T]]) {
        val dataSource: DataSource[T] = dataset.asInstanceOf[DataSource[T]]

        val inputFormat = dataSource.getInputFormat

        if (inputFormat.isInstanceOf[CollectionInputFormat[T]]) {
          val collectionInputFormat: CollectionInputFormat[T] = inputFormat.asInstanceOf[CollectionInputFormat[T]]

          collectionInputFormat.open(new GenericInputSplit(0,1))

          for (i <- 0 until index - 1) {
            collectionInputFormat.nextRecord(null.asInstanceOf[T])
          }

          collectionInputFormat.nextRecord(null.asInstanceOf[T])
        } else {
          throw new IllegalArgumentException("Dataset has to be of type CollectionDataSource and not " +
            dataset.getClass)
        }
      } else {
        throw new IllegalArgumentException("Dataset has to be of type CollectionDataSource and not " +
          dataset.getClass)
      }
    }
  }


}
