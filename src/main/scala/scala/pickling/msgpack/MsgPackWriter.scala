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
import xerial.core.log.Logger


abstract class MsgPackWriter extends Output[Array[Byte]] {
  def writeByteAndByte(b:Byte, v:Byte) : Unit
  def writeByteAndShort(b:Byte, v:Short) : Unit
  def writeByteAndInt(b:Byte, v:Int) : Unit
  def writeByteAndLong(b:Byte, v:Long) : Unit
  def writeByteAndFloat(b:Byte, v:Float) : Unit
  def writeByteAndDouble(b:Byte, v:Double) : Unit

  def writeByte(v:Byte) : Unit
  def writeShort(v:Short) : Unit
  def writeChar(v:Char) : Unit
  def writeInt(v:Int) : Unit
  def writeInt16(v:Int) : Unit
  def writeInt32(v:Int) : Unit

  def writeLong(v:Long) : Unit
  def writeFloat(v:Float) : Unit
  def writeDouble(v:Double) : Unit

  def write(b:Array[Byte]) : Unit = write(b, 0, b.length)
  def write(b:Array[Byte], off:Int, len:Int) : Unit
  def write(bb:ByteBuffer) : Unit

  import MsgPackCode._

  /**
   * Write a packed integer value
   * @param d
   */
  def packInt(d:Int) {
    if (d < -(1 << 5)) {
      if (d < -(1 << 15)) {
        // signed 32
        writeByteAndInt(F_INT32, d)
      }
      else if (d < -(1 << 7)) {
        // signed 16
        writeByteAndShort(F_INT16, d.toShort)
      }
      else {
        // signed 8
        writeByte(F_INT8)
        writeByte((d & 0xFF).toByte)
      }
    }
    else if (d < (1 << 7)) {
      // fixnum
      writeByte(d.toByte)
    }
    else {
      if (d < (1 << 8)) {
        // unsigned 8
        writeByteAndByte(F_UINT8, d.toByte)
      } else if (d < (1 << 16)) {
        // unsigned 16
        writeByteAndShort(F_UINT16, d.toShort)
      } else {
        // unsigned 32
        writeByteAndInt(F_UINT32, d)
      }
    }
  }



  def packLong(d:Long) {
    if (d < -(1L << 5)) {
      if (d < -(1L << 15)) {
        if(d < -(1L << 31)) {
          // signed 64
          writeByteAndLong(F_INT64, d)
        }
        else {
          // signed 32
          writeByteAndInt(F_INT32, d.toInt)
        }
      } else if (d < -(1 << 7)) {
        // signed 16
        writeByteAndShort(F_INT16, d.toShort)
      } else {
        // signed 8
        writeByteAndByte(F_INT8, d.toByte)
      }
    } else if (d < (1L << 7)) {
      // fixnum
      writeByte(d.toByte)
    } else {
      if (d < (1L << 8)) {
        // unsigned 8
        writeByteAndByte(F_UINT8, d.toByte)
      } else if (d < (1L << 16)) {
        // unsigned 16
        writeByteAndShort(F_UINT16, d.toShort)
      } else if (d < (1L << 32)) {
        // unsigned 32
        writeByteAndInt(F_UINT32, d.toInt)
      }
      else {
        // unsigned 64
        writeByteAndLong(F_UINT64, d)
      }
    }

  }

  def packString(s:String) {
    val bytes = s.getBytes("UTF-8")
    val len = bytes.length
    if(len < (1 << 5)) {
      writeByte((F_FIXSTR_PREFIX | (len & 0x1F)).toByte)
    } else if(len < (1 << 7)) {
      writeByte(F_STR8)
      writeByte((len & 0xFF).toByte)
    }
    else if(len < (1 << 15)) {
      writeByte(F_STR16)
      writeInt16(len)
    }
    else {
      writeByte(F_STR32)
      writeInt32(len)
    }
    write(bytes, 0, len)
  }

  def packByteArray(b:Array[Byte]) {
    val len = b.length
    val wroteBytes =
      if(len < (1 << 4)) {
        writeByte((F_ARRAY_PREFIX | len).toByte)
      }
      else if(len < (1 << 16)) {
        writeByte(F_ARRAY16)
        writeInt16(len)
      }
      else {
        writeByte(F_ARRAY32)
        writeInt32(len)
      }
    write(b, 0, len)
  }


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
  def writeInt16(v:Int) = {
    writeByte(((v >>> 8) & 0xFF).toByte)
    writeByte((v & 0xFF).toByte)
  }

  def writeInt32(v:Int) = {
    writeByte(((v >>> 24) & 0xFF).toByte)
    writeByte(((v >>> 16) & 0xFF).toByte)
    writeByte(((v >>> 8) & 0xFF).toByte)
    writeByte((v & 0xFF).toByte)
  }

  def writeChar(v:Char) = arr.putChar(v)
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

class MsgPackOutputBuffer() extends MsgPackWriter  {
  private var buffer : Array[Byte] = null
  private var capacity = 0
  private var size = 0
  private var wrap : ByteBuffer = null

  private def makeBuffer(size:Int) : Array[Byte] = {
    val newBuffer = new Array[Byte](size)
    if(this.size > 0)
      Array.copy(buffer, 0, newBuffer, 0, this.size)
    newBuffer
  }

  private def resize(size:Int) {
    buffer = makeBuffer(size)
    capacity = size
    wrap = ByteBuffer.wrap(buffer)
    wrap.position(this.size)
  }

  private def ensureSize(size:Int) {
    if(capacity < size || capacity == 0) {
      var newSize = if(capacity == 0) 16 else capacity * 2
      while(newSize < size) newSize *= 2
      resize(newSize)
    }
  }


  def result() = {
    if(size == 0)
      Array.empty[Byte]
    if(size == buffer.length)
      buffer
    else {
      val ret = new Array[Byte](size)
      Array.copy(buffer, 0, ret, 0, size)
      ret
    }
  }

  def put(obj: Array[Byte]) = ???

  def writeByteAndByte(b: Byte, v: Byte) = {
    writeByte(b)
    writeByte(v)
  }

  def writeByteAndShort(b: Byte, v: Short) = {
    writeByte(b)
    writeShort(v)
  }

  def writeByteAndInt(b: Byte, v: Int) = {
    writeByte(b)
    writeInt(v)
  }

  def writeByteAndLong(b: Byte, v: Long) = {
    writeByte(b)
    writeLong(v)
  }

  def writeByteAndFloat(b: Byte, v: Float) = {
    writeByte(b)
    writeFloat(v)
  }

  def writeByteAndDouble(b: Byte, v: Double) = {
    writeByte(b)
    writeDouble(v)
  }

  def writeByte(v: Byte) = {
    ensureSize(size + 1)
    wrap.put(v)
    size += 1
  }

  def writeShort(v: Short) = {
    ensureSize(size + 2)
    wrap.putShort(v)
    size += 2
  }

  def writeInt(v: Int) = {
    ensureSize(size + 4)
    wrap.putInt(v)
    size += 4
  }

  def writeInt16(v:Int) = {
    ensureSize(size + 2)
    writeByte(((v >>> 8) & 0xFF).toByte)
    writeByte((v & 0xFF).toByte)
    size += 2
  }

  def writeInt32(v:Int) = {
    ensureSize(size+4)
    writeByte(((v >>> 24) & 0xFF).toByte)
    writeByte(((v >>> 16) & 0xFF).toByte)
    writeByte(((v >>> 8) & 0xFF).toByte)
    writeByte((v & 0xFF).toByte)
    size += 4
  }


  def writeChar(v:Char) = {
    ensureSize(size + 2)
    wrap.putChar(v)
    size += 2
  }

  def writeLong(v: Long) = {
    ensureSize(size + 8)
    wrap.putLong(v)
    size += 8
  }

  def writeFloat(v: Float) = {
    ensureSize(size + 4)
    wrap.putFloat(v)
    size += 4
  }

  def writeDouble(v: Double) = {
    ensureSize(size + 8)
    wrap.putDouble(v)
    size += 8
  }

  def write(b: Array[Byte], off: Int, len: Int) = {
    ensureSize(size + len)
    wrap.put(b, off, len)
    size += len
  }

  def write(bb: ByteBuffer) = {
    val len = bb.remaining()
    ensureSize(size + len)
    wrap.put(bb)
    size += len
  }
}
