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
  import xerial.core.log.Logger

  case class MsgPackPickle(value:Array[Byte]) extends Pickle {
    type ValueType = Array[Byte]
    type PickleFormatType = MsgPackPickleFormat

    private def toHEX(b:Array[Byte]) = b.map(x => f"$x%x").mkString
    override def toString = s"""MsgPackPickle(${toHEX(value)})"""
  }


  class MsgPackPickleBuilder(format:MsgPackPickleFormat, out:MsgPackWriter) extends PBuilder with PickleTools with Logger {
    import format._
    import MsgPackCode._

    private var byteBuffer: MsgPackWriter = out

    private var pos = 0

    private[this] def mkByteBuffer(knownSize: Int): Unit = {
      if (byteBuffer == null) {
        byteBuffer = if (knownSize != -1) new MsgPackOutputArray(knownSize) else new MsgPackOutputBuffer
      }
    }


    /**
     * Pack and write an integer value then returns written byte size
     * @param d
     * @return byte size written
     */
    private def packInt(d:Int) : Int = {
      if (d < -(1 << 5)) {
        if (d < -(1 << 15)) {
          // signed 32
          byteBuffer.writeByteAndInt(F_INT32, d)
          5
        } else if (d < -(1 << 7)) {
          // signed 16
          byteBuffer.writeByteAndShort(F_INT16, d.toShort)
          3
        } else {
          // signed 8
          byteBuffer.writeByteAndByte(F_INT8, d.toByte)
          2
        }
      } else if (d < (1 << 7)) {
        // fixnum
        byteBuffer.writeByte(d.toByte)
        1
      } else {
        if (d < (1 << 8)) {
          // unsigned 8
          byteBuffer.writeByteAndByte(F_UINT8, d.toByte)
          2
        } else if (d < (1 << 16)) {
          // unsigned 16
          byteBuffer.writeByteAndShort(F_UINT16, d.toShort)
          3
        } else {
          // unsigned 32
          byteBuffer.writeByteAndInt(F_UINT32, d)
          5
        }
      }
    }

    private def packLong(d:Long) : Int = {
      if (d < -(1L << 5)) {
        if (d < -(1L << 15)) {
          if(d < -(1L << 31)) {
            // signed 64
            byteBuffer.writeByteAndLong(F_INT64, d)
            9
          }
          else {
            // signed 32
            byteBuffer.writeByteAndInt(F_INT32, d.toInt)
            5
          }
        } else if (d < -(1 << 7)) {
          // signed 16
          byteBuffer.writeByteAndShort(F_INT16, d.toShort)
          3
        } else {
          // signed 8
          byteBuffer.writeByteAndByte(F_INT8, d.toByte)
          2
        }
      } else if (d < (1L << 7)) {
        // fixnum
        byteBuffer.writeByte(d.toByte)
        1
      } else {
        if (d < (1L << 8)) {
          // unsigned 8
          byteBuffer.writeByteAndByte(F_UINT8, d.toByte)
          2
        } else if (d < (1L << 16)) {
          // unsigned 16
          byteBuffer.writeByteAndShort(F_UINT16, d.toShort)
          3
        } else if (d < (1L << 32)) {
          // unsigned 32
          byteBuffer.writeByteAndInt(F_UINT32, d.toInt)
          5
        }
        else {
          // unsigned 64
          byteBuffer.writeByteAndLong(F_UINT64, d)
          9
        }
      }

    }

    private def packString(s:String) = {
      val bytes = s.getBytes("UTF-8")
      val len = bytes.length
      if(len < (1 << 8))
        byteBuffer.writeByte(F_STR8)
      else if(len < (1 << 16))
        byteBuffer.writeByte(F_STR16)
      else
        byteBuffer.writeByte(F_STR32)
      byteBuffer.write(bytes, 0, len)
      1 + len
    }

    private def packByteArray(b:Array[Byte]) = {
      val len = b.length
      val wroteBytes =
        if(len < 15) {
          byteBuffer.writeByte((F_ARRAY_PREFIX | len).toByte)
          1
        }
        else if(len < (1 << 16)) {
          byteBuffer.writeByte(F_ARRAY16)
          byteBuffer.writeByte(((len >>> 8) & 0xFF).toByte)
          byteBuffer.writeByte((len & 0xFF).toByte)
          3
        }
        else {
          byteBuffer.writeByte(F_ARRAY32)
          byteBuffer.writeByte(((len >>> 24) & 0xFF).toByte)
          byteBuffer.writeByte(((len >>> 16) & 0xFF).toByte)
          byteBuffer.writeByte(((len >>> 8) & 0xFF).toByte)
          byteBuffer.writeByte((len & 0xFF).toByte)
          5
        }
      byteBuffer.write(b, 0, len)
      wroteBytes + len
    }



    def beginEntry(picklee: Any) = withHints { hints =>

      debug(s"hints: $hints")
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
          debug(s"encode type name: ${hints.tag.key}")
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
            pos + 2
          case KEY_SHORT =>
            byteBuffer.writeShort(picklee.asInstanceOf[Short])
            pos + 3
          case KEY_CHAR =>
            byteBuffer.writeChar(picklee.asInstanceOf[Char])
            pos + 3
          case KEY_INT =>
            pos + packInt(picklee.asInstanceOf[Int])
          case KEY_FLOAT =>
            byteBuffer.writeFloat(picklee.asInstanceOf[Float])
            pos + 5
          case KEY_LONG =>
            pos + packLong(picklee.asInstanceOf[Long])
          case KEY_DOUBLE =>
            byteBuffer.writeDouble(picklee.asInstanceOf[Double])
            pos + 9
          case KEY_SCALA_STRING | KEY_JAVA_STRING =>
            pos + packString(picklee.asInstanceOf[String])
          case KEY_ARRAY_BYTE =>
            pos + packByteArray(picklee.asInstanceOf[Array[Byte]])
          case _ =>
            if(hints.isElidedType) {
              warn(s"TODO: elided type")
              pos
            }
            else
              pos
        }
      }

      this
    }

    @inline def putField(name: String, pickler: PBuilder => Unit) = {
      pickler(this)
      this
    }

    def endEntry() : Unit = {

     /* do nothing */

    }

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
    import format._

    private val in = new MsgPackByteArrayReader(arr)
    private var pos                          = 0
    private var _lastTagRead: FastTypeTag[_] = null
    private var _lastTypeStringRead: String  = null

    private def lastTagRead: FastTypeTag[_] =
      if (_lastTagRead != null)
        _lastTagRead
      else {
        // assume _lastTypeStringRead != null
        _lastTagRead = FastTypeTag(mirror, _lastTypeStringRead)
        _lastTagRead
      }

    def beginEntry() : FastTypeTag[_] = {
      beginEntryNoTag()
      lastTagRead
    }

    def beginEntryNoTag() = ???


    def atPrimitive = primitives.contains(lastTagRead.key)

    def readPrimitive() : Any = {
      var newpos = pos
      val res = lastTagRead.key match {
        case KEY_NULL => null
        case KEY_REF =>  null // TODO
        case KEY_BYTE =>
          newpos = pos + 1
          in.readByte

      }
      res
    }

    def atObject = !atPrimitive

    def readField(name: String) : MsgPackPickleReader = this

    def endEntry() : Unit = { /* do nothing */ }

    def beginCollection() : PReader = this

    def readLength() : Int = {
      in.decodeInt
    }

    def readElement() : PReader = this

    def endCollection() : Unit = { /* do nothing */ }
  }

  object MsgPackCode {
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

    val KEY_ARRAY_BYTE    = FastTypeTag.ArrayByte.key
    val KEY_ARRAY_SHORT   = FastTypeTag.ArrayShort.key
    val KEY_ARRAY_CHAR    = FastTypeTag.ArrayChar.key
    val KEY_ARRAY_INT     = FastTypeTag.ArrayInt.key
    val KEY_ARRAY_LONG    = FastTypeTag.ArrayLong.key
    val KEY_ARRAY_BOOLEAN = FastTypeTag.ArrayBoolean.key
    val KEY_ARRAY_FLOAT   = FastTypeTag.ArrayFloat.key
    val KEY_ARRAY_DOUBLE  = FastTypeTag.ArrayDouble.key

    val KEY_REF = FastTypeTag.Ref.key


    val primitives = Set(KEY_NULL, KEY_REF, KEY_BYTE, KEY_SHORT, KEY_CHAR, KEY_INT, KEY_LONG, KEY_BOOLEAN, KEY_FLOAT, KEY_DOUBLE, KEY_UNIT, KEY_SCALA_STRING, KEY_JAVA_STRING, KEY_ARRAY_BYTE, KEY_ARRAY_SHORT, KEY_ARRAY_CHAR, KEY_ARRAY_INT, KEY_ARRAY_LONG, KEY_ARRAY_BOOLEAN, KEY_ARRAY_FLOAT, KEY_ARRAY_DOUBLE)

    type PickleType = MsgPackPickle
    type OutputType = MsgPackWriter

    def createReader(pickle: PickleType, mirror: Mirror) = new MsgPackPickleReader(pickle.value, mirror, this)

    def createBuilder() = new MsgPackPickleBuilder(this, null)
    def createBuilder(out: MsgPackWriter) = new MsgPackPickleBuilder(this, out)
  }


}
