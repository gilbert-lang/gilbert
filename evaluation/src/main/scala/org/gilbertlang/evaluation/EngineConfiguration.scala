package org.gilbertlang.evaluation

case class EngineConfiguration(val parallelism: Int, val engine: Engines.Value, val jobmanager: String,
                               val jobmanagerPort: Int, val optTP: Boolean, val optMMReordering: Boolean, val tries: Int,
                               val outputPath: String, val checkpointDir: String, val iterationsUntilCheckpoint: Int) {

}
