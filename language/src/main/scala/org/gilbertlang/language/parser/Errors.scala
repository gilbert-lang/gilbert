package org.gilbertlang.language.parser

class ParseError(msg: String) extends Error(msg){
  def this() = this("Parsing error")
}