package org.gilbertlang.runtime.execution.stratosphere

import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.Value
import org.gilbertlang.runtimeMacros.linalg.{BooleanSubmatrix, Submatrix}


class ValueWrapper(var value: Any) extends Value {
  import ValueWrapper._

  def this() = this(null)

  def getAs[T]: T = value.asInstanceOf[T]

  override def write(out: DataOutputView){
    value match {
      case x: Double =>
        out.writeUTF(doubleID)
        out.writeDouble(x)
      case x: Boolean =>
        out.writeUTF(booleanID)
        out.writeBoolean(x)
      case x: Int =>
        out.writeUTF(intID)
        out.writeInt(x)
      case x:String =>
        out.writeUTF(stringID)
        out.writeUTF(x)
      case x:Submatrix =>
        out.writeUTF(submatrixID)
        x.write(out)
      case x:BooleanSubmatrix =>
        out.writeUTF(submatrixBooleanID)
        x.write(out)
    }
  }

  override def read(in: DataInputView){
    val tpe = in.readUTF()

    tpe match {
      case `doubleID` => value = in.readDouble()
      case `booleanID` => value = in.readBoolean()
      case `intID` => value = in.readInt()
      case `stringID` => value = in.readUTF()
      case `submatrixID` =>
        val submatrix = new Submatrix()
        submatrix.read(in)
        value = submatrix
      case `submatrixBooleanID` =>
        val submatrix = new BooleanSubmatrix()
        submatrix.read(in)
        value = submatrix

    }
  }
}

object ValueWrapper{
  def apply(value: Any) = new ValueWrapper(value)

  val doubleID = "double"
  val intID = "int"
  val booleanID = "boolean"
  val stringID = "string"
  val submatrixID = "submatrix"
  val submatrixBooleanID = "submatrixBoolean"
}
