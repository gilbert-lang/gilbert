package org.gilbertlang.runtimeMacros.linalg

import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.Value

trait BooleanMatrix extends Value {
  def rows: Int
  def cols: Int

  def activeSize: Int

  def apply(row: Int, col: Int): Boolean
  def update(coord: (Int, Int), value: Boolean): Unit

  def activeIterator:Iterator[((Int,Int), Boolean)]

  def copy: BooleanMatrix

  def &(op: BooleanMatrix): BooleanMatrix
  def &(op: Boolean): BooleanMatrix
  def :&(op: BooleanMatrix): BooleanMatrix = this.&(op)
  def :&(op: Boolean): BooleanMatrix = this.&(op)
  def :|(op: BooleanMatrix): BooleanMatrix
  def :|(op: Boolean): BooleanMatrix

  def t: BooleanMatrix

  override def write(out: DataOutputView): Unit = {

  }

  override def read(in: DataInputView): Unit = {

  }
}
