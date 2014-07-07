package org.gilbertlang.runtimeMacros.linalg

@SerialVersionUID(1)
case class RuntimeConfiguration(val blocksize:Int = 1,val densityThreshold: Double= 0.6,
                                val compilerHints:Boolean = true, val outputPath: Option[String] = None,
                                val checkpointDir: Option[String] = None, val iterationsUntilCheckpoint: Int = 0,
                                val preserveHint: Boolean = false, val verboseWrite:Boolean = false)
  extends
  Serializable {}