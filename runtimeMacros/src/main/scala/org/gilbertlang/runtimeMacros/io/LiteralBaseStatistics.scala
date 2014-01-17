package org.gilbertlang.runtimeMacros.io

import eu.stratosphere.api.common.io.statistics.BaseStatistics

case class LiteralBaseStatistics(totalInputSize: Long, numberOfRecords: Long, averageRecordWidth: Double) extends BaseStatistics{
  override def getTotalInputSize = totalInputSize
  override def getNumberOfRecords = numberOfRecords
  override def getAverageRecordWidth = averageRecordWidth.toFloat
}