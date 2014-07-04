package org.gilbertlang.runtime

import org.gilbertlang.runtime.Executables.Executable
import org.gilbertlang.runtimeMacros.linalg.RuntimeConfiguration

trait ExecutionEngine {
  def stop(): Unit

  def execute(program: Executable, configuration: RuntimeConfiguration): Double
}
