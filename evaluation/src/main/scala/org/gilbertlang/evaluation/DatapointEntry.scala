package org.gilbertlang.evaluation

case class DatapointEntry(val time: Double, val error: Double, val dop: Int, val blocksize: Int,
                          val densityThreshold: Double, dataset: Map[String, String]) {

}
