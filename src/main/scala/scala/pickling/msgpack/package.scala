package scala.pickling

import scala.reflect.runtime.universe.Mirror
import scala.language.implicitConversions

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

    /**
     * Pack and write an integer value then returns written byte size
     * @param d
     * @return byte size written
     */
    private def packInt(d:Int) : Int = {
      if (d < -(1 << 5)) {
        if (d < -(1 << 15)) {
          // signed 32
          out.writeByteAndInt(F_INT32, d)
          5
        } else if (d < -(1 << 7)) {
          // signed 16
          out.writeByteAndShort(F_INT16, d.toShort)
          3
        } else {
          // signed 8
          out.writeByteAndByte(F_INT8, d.toByte)
          2
        }
      } else if (d < (1 << 7)) {
        // fixnum
        out.writeByte(d.toByte)
        1
      } else {
        if (d < (1 << 8)) {
          // unsigned 8
          out.writeByteAndByte(F_UINT8, d.toByte)
          2
        } else if (d < (1 << 16)) {
          // unsigned 16
          out.writeByteAndShort(F_UINT16, d.toShort)
          3
        } else {
          // unsigned 32
          out.writeByteAndInt(F_UINT32, d)
          5
        }
      }
    }

    private def packLong(d:Long) : Int = {
      if (d < -(1L << 5)) {
        if (d < -(1L << 15)) {
          if(d < -(1L << 31)) {
            // signed 64
            out.writeByteAndLong(F_INT64, d)
            9
          }
          else {
            // signed 32
            out.writeByteAndInt(F_INT32, d.toInt)
            5
          }
        } else if (d < -(1 << 7)) {
          // signed 16
          out.writeByteAndShort(F_INT16, d.toShort)
          3
        } else {
          // signed 8
          out.writeByteAndByte(F_INT8, d.toByte)
          2
        }
      } else if (d < (1L << 7)) {
        // fixnum
        out.writeByte(d.toByte)
        1
      } else {
        if (d < (1L << 8)) {
          // unsigned 8
          out.writeByteAndByte(F_UINT8, d.toByte)
          2
        } else if (d < (1L << 16)) {
          // unsigned 16
          out.writeByteAndShort(F_UINT16, d.toShort)
          3
        } else if (d < (1L << 32)) {
          // unsigned 32
          out.writeByteAndInt(F_UINT32, d.toInt)
          5
        }
        else {
          // unsigned 64
          out.writeByteAndLong(F_UINT64, d)
          9
        }
      }

    }

    private def packString(s:String) = {
      val bytes = s.getBytes("UTF-8")
      val len = bytes.length
      if(len < (1 << 8))
        out.writeByte(F_STR8)
      else if(len < (1 << 16))
        out.writeByte(F_STR16)
      else
        out.writeByte(F_STR32)
      out.write(bytes, 0, len)
      1 + len
    }



    def beginEntry(picklee: Any) = withHints { hints =>
      mkByteBuffer(hints.knownSize)

      if(picklee == null)
        byteBuffer.writeByte(F_NULL)
      else if (hints.oid != -1) {
        // Has an object ID
        val oid = hints.oid
        // TODO Integer compaction
        byteBuffer.writeByte(F_FIXEXT4)
        byteBuffer.writeByte(F_OBJREF)
        byteBuffer.writeInt(hints.oid)
      } else {
        if(!hints.isElidedType) {
          // Type name is present
          val tpeBytes = hints.tag.key.getBytes("UTF-8")
          packInt(tpeBytes.length)
          byteBuffer.write(tpeBytes)
        }

        pos = hints.tag.key match {
          case KEY_NULL =>
            byteBuffer.writeByte(F_NULL)
            pos + 1
          case KEY_BYTE =>
            byteBuffer.writeByte(picklee.asInstanceOf[Byte])
            pos + 1
          case KEY_SHORT =>
            byteBuffer.writeShort(picklee.asInstanceOf[Short])
            pos + 2
          case KEY_CHAR =>
            byteBuffer.writeChar(picklee.asInstanceOf[Char])
            pos + 2
          case KEY_INT =>
            pos + packInt(picklee.asInstanceOf[Int])
          case KEY_LONG =>
            pos + packLong(picklee.asInstanceOf[Long])
          case KEY_DOUBLE =>
            byteBuffer.writeDouble(picklee.asInstanceOf[Double])
            pos + 4
          case KEY_SCALA_STRING | KEY_JAVA_STRING =>
            packString(picklee.asInstanceOf[String])
            
          //case F_
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
      if(length < (1 << 4))
        byteBuffer.writeByte((F_ARRAY_PREFIX | length).toByte)
      else if(length < (1 << 16)) {
        byteBuffer.writeByte(F_ARRAY16)
        byteBuffer.writeShort(length.toShort)
      }
      else {
        byteBuffer.writeByte(F_ARRAY32)
        byteBuffer.writeInt(length)
      }
      this
    }

    def putElement(pickler: (PBuilder) => Unit) = {
      pickler(this)
      this
    }

    def endCollection() : Unit = {
      // do nothing
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

    val KEY_NULL    = FastTypeTag.Null.key
    val KEY_BYTE    = FastTypeTag.Byte.key
    val KEY_SHORT   = FastTypeTag.Short.key
    val KEY_CHAR    = FastTypeTag.Char.key
    val KEY_INT     = FastTypeTag.Int.key
    val KEY_LONG    = FastTypeTag.Long.key
    val KEY_BOOLEAN = FastTypeTag.Boolean.key
    val KEY_FLOAT   = FastTypeTag.Float.key
    val KEY_DOUBLE  = FastTypeTag.Double.key
    val KEY_UNIT    = FastTypeTag.Unit.key

    val KEY_SCALA_STRING = FastTypeTag.ScalaString.key
    val KEY_JAVA_STRING  = FastTypeTag.JavaString.key


    val F_NULL : Byte = 0xC0.toByte
    val F_OBJREF : Byte = 1.toByte

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

    val F_ARRAY_PREFIX : Byte = 0x90.toByte
    val F_ARRAY16 : Byte = 0xDC.toByte
    val F_ARRAY32 : Byte = 0xDD.toByte

    val F_STR8 : Byte = 0xD9.toByte
    val F_STR16 : Byte = 0xDA.toByte
    val F_STR32 : Byte = 0xDB.toByte

    type PickleType = MsgPackPickle
    type OutputType = MsgPackWriter

    def createReader(pickle: PickleType, mirror: Mirror) = new MsgPackPickleReader(pickle.value, mirror, this)

    def createBuilder() : PBuilder = new MsgPackPickleBuilder(this, null)

    def createBuilder(out: MsgPackPickleFormat#OutputType) = new MsgPackPickleBuilder(this, out)
  }


}
