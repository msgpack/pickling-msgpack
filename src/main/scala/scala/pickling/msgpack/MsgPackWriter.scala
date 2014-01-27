//--------------------------------------
//
// MsgPackWriter.scala
// Since: 2014/01/27 23:12
//
//--------------------------------------

package scala.pickling.msgpack

import scala.pickling.Output
import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer


abstract class MsgPackWriter extends Output[Array[Byte]] {
  def writeByteAndByte(b:Byte, v:Byte) : Unit
  def writeByteAndShort(b:Byte, v:Short) : Unit
  def writeByteAndInt(b:Byte, v:Int) : Unit
  def writeByteAndLong(b:Byte, v:Long) : Unit
  def writeByteAndFloat(b:Byte, v:Float) : Unit
  def writeByteAndDouble(b:Byte, v:Double) : Unit

  def writeByte(v:Byte) : Unit
  def writeShort(v:Short) : Unit
  def writeInt(v:Int) : Unit
  def writeLong(v:Long) : Unit
  def writeFloat(v:Float) : Unit
  def writeDouble(v:Double) : Unit

  def write(b:Array[Byte], off:Int, len:Int) : Unit
  def write(bb:ByteBuffer) : Unit
}

class MsgPackOutputArray(size:Int) extends MsgPackWriter {
  private val arr = ByteBuffer.allocate(size)

  def result() = arr.array()
  def put(obj: Array[Byte]) = ???

  def writeByteAndByte(b: Byte, v: Byte) = {
    arr.put(b)
    arr.put(v)
  }
  def writeByteAndShort(b: Byte, v: Short) = {
    arr.put(b)
    arr.putShort(v)
  }
  def writeByteAndInt(b: Byte, v: Int) = {
    arr.put(b)
    arr.putInt(v)
  }
  def writeByteAndLong(b: Byte, v: Long) = {
    arr.put(b)
    arr.putLong(v)
  }
  def writeByteAndFloat(b: Byte, v: Float) = {
    arr.put(b)
    arr.putFloat(v)
  }
  def writeByteAndDouble(b: Byte, v: Double) = {
    arr.put(b)
    arr.putDouble(v)
  }

  def writeByte(v: Byte) = arr.put(v)
  def writeShort(v: Short) = arr.putShort(v)
  def writeInt(v: Int) = arr.putInt(v)
  def writeLong(v: Long) = arr.putLong(v)
  def writeFloat(v: Float) = arr.putFloat(v)
  def writeDouble(v: Double) = arr.putDouble(v)

  def write(b: Array[Byte], off: Int, len: Int) = {
    arr.put(b, off, len)
  }

  def write(bb: ByteBuffer) = {
    arr.put(bb)
  }
}

class MsgPackOutputBuffer() extends MsgPackWriter {
  private var buffer : Array[Byte]
  private var cursor = 0
  private var bufferSize = 0
  private var wrap : ByteBuffer

  private def allocateNewBuffer {
    buffer = new Array[Byte](bufferSize)
    wrap = ByteBuffer.wrap(buffer)
  }

  private def reserve(len:Int) {
    if(buffer == null)
      allocateNewBuffer
    else {
      if(bufferSize - cursor < len) {

      }
    }
  }

  def result() = ???

  def put(obj: Array[Byte]) = ???

  def writeByteAndByte(b: Byte, v: Byte) = ???

  def writeByteAndShort(b: Byte, v: Short) = ???

  def writeByteAndInt(b: Byte, v: Int) = ???

  def writeByteAndLong(b: Byte, v: Long) = ???

  def writeByteAndFloat(b: Byte, v: Float) = ???

  def writeByteAndDouble(b: Byte, v: Double) = ???

  def writeByte(v: Byte) = ???

  def writeShort(v: Short) = ???

  def writeInt(v: Int) = ???

  def writeLong(v: Long) = ???

  def writeFloat(v: Float) = ???

  def writeDouble(v: Double) = ???

  def write(b: Array[Byte], off: Int, len: Int) = ???

  def write(bb: ByteBuffer) = ???
}
