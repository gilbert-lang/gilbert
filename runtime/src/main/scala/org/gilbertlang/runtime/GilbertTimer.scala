package org.gilbertlang.runtime

import java.io.OutputStream

import scala.util.matching.Regex

class GilbertTimer(val pattern: Regex) extends OutputStream{
  var totalTime:Double = 0;

  override def write(value: Int){}

  override def write(b: Array[Byte], off: Int, len: Int) {
    val line = new String(b, off, len)
    pattern.findFirstMatchIn(line).foreach{
      m =>
        if(m.groupCount == 1){
          totalTime += m.group(1).toDouble
        }
    }
  }
}
