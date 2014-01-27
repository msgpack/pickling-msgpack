package scala.pickling

import scala.reflect.runtime.universe.Mirror

/**
 * @author Taro L. Saito
 */
package object msgpack {

  implicit val msgpackFormat = new MsgPackPickleFormat
  implicit def toMsgPackPickle(value:Array[Byte]) : MsgPackPickle = MsgPackPickle(value)
}

package msgpack {

import scala.pickling.binary.{ByteArrayBuffer, ByteArray, BinaryPickleFormat}
import scala.collection.mutable.ArrayBuffer
import java.nio.ByteBuffer

case class MsgPackPickle(value:Array[Byte]) extends Pickle {
    type ValueType = Array[Byte]
    type PickleFormatType = BinaryPickleFormat

    private def toHEX(b:Array[Byte]) = b.map(x => f"$x%x").mkString
    override def toString = s"""MsgPackPickle(${toHEX(value)})"""
  }



  class MsgPackPickleBuilder(format:MsgPackPickleFormat, out:MsgPackWriter) extends PBuilder with PickleTools {
    import format._

    private var byteBuffer: MsgPackWriter = out

    private var pos = 0

    @inline private[this] def mkByteBuffer(knownSize: Int): Unit =
      if (byteBuffer == null) {
        byteBuffer = if (knownSize != -1) new MsgPackOutputArray(knownSize) else new MsgPackOutputBuffer      }

    private def encodeInt(d:Int) = {
      if (d < -(1 << 5)) {
        if (d < -(1 << 15)) {
          // signed 32
          out.writeByteAndInt((byte) 0xd2, d);
        } else if (d < -(1 << 7)) {
          // signed 16
          out.writeByteAndShort((byte) 0xd1, (short) d);
        } else {
          // signed 8
          out.writeByteAndByte((byte) 0xd0, (byte) d);
        }
      } else if (d < (1 << 7)) {
        // fixnum
        out.writeByte((byte) d);
      } else {
        if (d < (1 << 8)) {
          // unsigned 8
          out.writeByteAndByte((byte) 0xcc, (byte) d);
        } else if (d < (1 << 16)) {
          // unsigned 16
          out.writeByteAndShort((byte) 0xcd, (short) d);
        } else {
          // unsigned 32
          out.writeByteAndInt((byte) 0xce, d);
        }
      }
    }


    def beginEntry(picklee: Any) = withHints { hints =>
      mkByteBuffer(hints.knownSize)

      if(picklee == null) {
        byteBuffer.encodeByteTo(pos, F_NULL)
        pos += 1
      }
      else if (hints.oid != -1) {
        // Has object ID
        val oid = hints.oid
        // TODO Integer compaction
        byteBuffer.encodeByteTo(pos, F_FIXEXT4)
        pos += 1
        byteBuffer.encodeByteTo(pos, F_OBJREF)
        pos += 1
        byteBuffer.encodeIntTo(pos, hints.oid)
        pos += 4
      } else {
        if(!hints.isElidedType) {
          // Type name is present
          val tpeBytes = hints.tag.key.getBytes("UTF-8")
          byteBuffer.encodeByteTo(pos, )


        }


      }

      this
    }

    @inline def putField(name: String, pickler: PBuilder => Unit) = {
      pickler(this)
      this
    }

    def endEntry() : Unit = { /* do nothing */ }

    def beginCollection(length: Int) : PBuilder = {
      // FIXME
      byteBuffer.encodeIntTo(pos, length)
      pos += 4
      this
    }

    def putElement(pickler: (PBuilder) => Unit) = {
      pickler(this)
      this
    }

    def endCollection() : Unit = {

    }

    def result() = {
      MsgPackPickle(byteBuffer.result())
    }
  }


  class MsgPackPickleReader(arr:Array[Byte], val mirror:Mirror, format: MsgPackPickleFormat) extends PReader with PickleTools {
    def beginEntry() = ???

    def beginEntryNoTag() = ???

    def atPrimitive = ???

    def readPrimitive() = ???

    def atObject = ???

    def readField(name: String) = ???

    def endEntry() = ???

    def beginCollection() = ???

    def readLength() = ???

    def readElement() = ???

    def endCollection() = ???
  }

  class MsgPackPickleFormat extends PickleFormat {

    val F_NULL : Byte = 0xC0.toByte
    val F_OBJREF : Byte = 0.toByte

    val F_UINT8 : Byte = 0xCC.toByte
    val F_UINT16 : Byte = 0xCD.toByte
    val F_UINT32 : Byte = 0xCE.toByte
    val F_UINT64 : Byte = 0xCF.toByte
    
    val F_INT8 : Byte = 0xD0.toByte
    val F_INT16 : Byte = 0xD1.toByte
    val F_INT32 : Byte = 0xD2.toByte
    val F_INT64 : Byte = 0xD3.toByte


    val F_FIXEXT1 : Byte = 0xD4.toByte
    val F_FIXEXT2 : Byte = 0xD5.toByte
    val F_FIXEXT4 : Byte = 0xD6.toByte
    val F_FIXEXT8 : Byte = 0xD7.toByte
    val F_FIXEXT16 : Byte = 0xD8.toByte



    type PickleType = MsgPackPickle
    type OutputType = MsgPackWriter

    def createReader(pickle: PickleType, mirror: Mirror) = new MsgPackPickleReader(pickle.value, mirror, this)

    def createBuilder() : PBuilder = new MsgPackPickleBuilder(this, null)

    def createBuilder(out: MsgPackPickleFormat#OutputType) = new MsgPackPickleBuilder(this, out)
  }


}
