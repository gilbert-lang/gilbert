package org.gilbertlang.language.format

trait Formatter[T] {
  def prettyString(element: T): String
  def prettyPrint(element: T) {
    println(prettyString(element))
  }
}