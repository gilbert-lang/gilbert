package org.gilbertlang.runtimeMacros.linalg

import java.io.{DataInput, DataOutput}

import eu.stratosphere.types.Value

trait BooleanMatrix extends Value {
  def rows: Int
  def cols: Int

  def apply(row: Int, col: Int): Boolean
  def update(coord: (Int, Int), value: Boolean): Unit

  def activeIterator:Iterator[((Int,Int), Boolean)]

  def copy: BooleanMatrix

  def &(op: BooleanMatrix): BooleanMatrix
  def &(op: Boolean): BooleanMatrix

  def t: BooleanMatrix

  def write(out: DataOutput): Unit = {

  }

  def read(in: DataInput): Unit = {

  }
}
