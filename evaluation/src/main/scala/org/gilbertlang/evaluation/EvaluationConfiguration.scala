package org.gilbertlang.evaluation

case class EvaluationConfiguration(val engine: Engines.Value, val mathBackend: MathBackend.Value, val tries: Int,
                                   val optTP: Boolean,
                                   val optMMReordering :Boolean) {
}
