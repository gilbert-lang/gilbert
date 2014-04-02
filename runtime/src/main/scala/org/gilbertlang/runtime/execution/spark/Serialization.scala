/**
 * gilbert - Distributed Linear Algebra on Sparse Matrices
 * Copyright (C) 2013  Sebastian Schelter, Till Rohrmann
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gilbertlang.runtime.execution.spark

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.hadoop.io.{DataInputBuffer, DataOutputBuffer, Writable}
import org.apache.spark.serializer.KryoRegistrator
import scala.reflect.ClassTag
import scala.reflect.classTag

/**
 *
 * @author dmitriy
 */
class MahoutKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo) = {
//    kryo.addDefaultSerializer(classOf[Vector], new WritableKryoSerializer[Vector, VectorWritable])
//    kryo.addDefaultSerializer(classOf[DenseVector], new WritableKryoSerializer[Vector, VectorWritable])
//    kryo.addDefaultSerializer(classOf[Matrix], new WritableKryoSerializer[Matrix, MatrixWritable])
  }
}

class WritableKryoSerializer[V <% Writable, W <: Writable <% V : ClassTag] extends Serializer[V] {

  def write(kryo: Kryo, out: Output, v: V) = {
    val dob = new DataOutputBuffer()
    v.write(dob)
    dob.close()

    out.writeInt(dob.getLength)
    out.write(dob.getData, 0, dob.getLength)
  }

  def read(kryo: Kryo, in: Input, vClazz: Class[V]): V = {
    val dib = new DataInputBuffer()
    val len = in.readInt()
    val data = new Array[Byte](len)
    in.read(data)
    dib.reset(data, len)
    val w: W = classTag[W].runtimeClass.newInstance().asInstanceOf[W]
    w.readFields(dib)
    w
  }
}