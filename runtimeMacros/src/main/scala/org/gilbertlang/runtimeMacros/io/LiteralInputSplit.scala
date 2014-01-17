package org.gilbertlang.runtimeMacros.io

import eu.stratosphere.core.io.InputSplit
import java.io.DataInput
import java.io.IOException
import java.io.DataOutput
import eu.stratosphere.core.io.GenericInputSplit

class LiteralInputSplit(splitNumber: Int, splitStartIndex: Int, splitEndIndex: Int) 
extends GenericInputSplit {
  number = splitNumber
  var startIndex = splitStartIndex
  var endIndex = splitEndIndex
  
  def this() = this(-1,-1,-1)
  
  @throws(classOf[IOException])
  override def read(in: DataInput) {
    super.read(in)
    startIndex = in.readInt()
    endIndex = in.readInt()
  }
  
  @throws(classOf[IOException])
  override def write(out: DataOutput){
    super.write(out)
    out.writeInt(startIndex)
    out.writeInt(endIndex)
  }
  
  override def toString = {
    "[" + number + "]" + startIndex + "->" + endIndex 
  }
}