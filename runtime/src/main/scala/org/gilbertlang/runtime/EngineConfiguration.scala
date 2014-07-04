package org.gilbertlang.runtime

case class EngineConfiguration(val master: String = "", val port: Int = -1, val appName: String = "App",
                               val parallelism: Int = 1, val jars: List[String] = List()) {

}
