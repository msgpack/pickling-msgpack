package scala.pickling

import scala.reflect.runtime.universe.Mirror
import scala.language.implicitConversions
import scala.collection.generic.CanBuildFrom
import xerial.core.log.Logger


/**
 * @author Taro L. Saito
 */
package object msgpack {

  implicit val msgpackFormat = new MsgPackPickleFormat
  implicit def toMsgPackPickle(value:Array[Byte]) : MsgPackPickle = MsgPackPickle(value)

  implicit def msgpackImmMapPickler[K:FastTypeTag, V:FastTypeTag](implicit keyPickler: SPickler[K], keyUnpickler:Unpickler[K], valuePickler: SPickler[V], valueUnpickler:Unpickler[V], pairTag: FastTypeTag[(K, V)], collTag: FastTypeTag[Map[K, V]], format: PickleFormat, cbf: CanBuildFrom[Map[K, V], (K, V), Map[K, V]])
    : SPickler[Map[K, V]] with Unpickler[Map[K, V]] = new SPickler[Map[K, V]] with Unpickler[Map[K, V]] with PickleTools with Logger {

    override val format: PickleFormat = msgpackFormat

    import scala.reflect.runtime.universe._
    import scala.pickling.internal._

    val keyTag = implicitly[FastTypeTag[K]]
    val valueTag = implicitly[FastTypeTag[V]]
    var keyIsPrimitive = false
    var valueIsPrimitive = false
    pairTag.tpe match {
      case tr @ TypeRef(prefix, symbol, List(ktpe, vtpe)) =>
        trace(s"key: $ktpe ${ktpe.isEffectivelyPrimitive}, value: $vtpe ${vtpe.isEffectivelyPrimitive}")
        keyIsPrimitive = ktpe.isEffectivelyPrimitive
        valueIsPrimitive = vtpe.isEffectivelyPrimitive
        trace(s"keyTag $keyTag, valueTag $valueTag")
      case _ =>
    }

    override def pickle(coll: Map[K, V], builder: PBuilder): Unit = {
      trace(s"custom map pickler")
      builder.hintTag(collTag)
      builder.beginEntry(coll)
      builder.beginCollection(coll.size)


      trace(s"elem type: ${pairTag.tpe}")
      for((k, v) <- coll) {
        // output key
        builder.hintTag(keyTag)
        if(keyIsPrimitive) {
          builder.hintStaticallyElidedType()
          builder.pinHints()
        }
        keyPickler.pickle(k, builder)
        if(keyIsPrimitive) {
          builder.unpinHints()
        }

        // output value
        builder.hintTag(valueTag)
        if(valueIsPrimitive) {
          builder.hintStaticallyElidedType()
          builder.pinHints()
        }
        valuePickler.pickle(v, builder)
        if(valueIsPrimitive) {
          builder.unpinHints()
        }
      }

      builder.endCollection
      builder.endEntry()

    }
    override def unpickle(tag: => FastTypeTag[_], preader: PReader): Any = {
      trace(s"custom unpickler")

      val reader = preader.beginCollection()
      val length = reader.readLength()
      val builder = cbf.apply()
      builder.sizeHint(length)
      var i = 0
      while(i < length) {
        reader.readElement()
        reader.beginEntryNoTag()
        val key = keyUnpickler.unpickle(keyTag, reader)
        reader.endEntry()

        reader.readElement()
        reader.beginEntryNoTag()
        val value = valueUnpickler.unpickle(valueTag, reader)
        reader.endEntry()

        builder += ((key, value).asInstanceOf[(K, V)])
        i += 1
      }
      reader.endCollection()

      builder.result
    }

  }




  private[msgpack] def toHEX(b:Array[Byte]) = b.map(x => f"$x%02x").mkString
}

package msgpack {

  import scala.pickling.binary.{ByteArrayBuffer, ByteArray, BinaryPickleFormat}
  import xerial.core.log.Logger
import scala.collection.immutable.Queue
import xerial.lens.TypeUtil

private sealed trait CodeState
private case object CodeMap extends CodeState
private case object CodeDefault extends CodeState

case class MsgPackPickle(value:Array[Byte]) extends Pickle {
    type ValueType = Array[Byte]
    type PickleFormatType = MsgPackPickleFormat
    override def toString = s"""MsgPackPickle(${toHEX(value)})"""
  }


  class MsgPackPickleBuilder(format:MsgPackPickleFormat, out:MsgPackWriter) extends PBuilder with PickleTools with Logger {
    import format._
    import MsgPackCode._

    private var byteBuffer: MsgPackWriter = out
    private val stateStackOfElidedType = new scala.collection.mutable.Stack[CodeState]()

    private[this] def mkByteBuffer(knownSize: Int): Unit = {
      if (byteBuffer == null) {
        byteBuffer = if (knownSize != -1) new MsgPackOutputArray(knownSize) else new MsgPackOutputBuffer
      }
    }

    private var currentHint : Hints = null

    def beginEntry(picklee: Any) = withHints { hints =>

      trace(s"hints: $hints")
      currentHint = hints
      mkByteBuffer(hints.knownSize)

      var encodeTypeName = true

      if(picklee == null)
        byteBuffer.writeByte(F_NULL)
      else if (hints.oid != -1) {
        // Has an object ID
        val oid = hints.oid
        // TODO Integer compaction
        byteBuffer.writeByte(F_FIXEXT4)
        byteBuffer.writeByte(F_EXT_OBJREF)
        byteBuffer.writeInt(hints.oid)
      } else {
        if(!hints.isElidedType) {
          // Type name is present
          val tpe = hints.tag.tpe
          val cls = hints.tag.mirror.runtimeClass(tpe)

          def isTuple(c:Class[_]) = {
            classOf[Product].isAssignableFrom(c) && c.getSimpleName.startsWith("Tuple")
          }

          trace(s"encode type name: ${hints.tag.tpe} runtime class: $cls (${isTuple(cls)}), ${stateStackOfElidedType.headOption}")
          stateStackOfElidedType.headOption match {
            case Some(CodeMap) if isTuple(cls) =>
              trace("encode tuple")
              encodeTypeName = false
            case s if TypeUtil.isMap(cls) => {
              encodeTypeName = false
            }
            case _ =>
              val tpeBytes = hints.tag.key.getBytes("UTF-8")
              tpeBytes.length match {
                case l if l < (1 << 7) =>
                  byteBuffer.writeByte(F_EXT8)
                  byteBuffer.writeByte((l & 0xFF).toByte)
                case l if l < (1 << 15) =>
                  byteBuffer.writeByte(F_EXT16)
                  byteBuffer.writeInt16(l)
                case l =>
                  byteBuffer.writeByte(F_EXT32)
                  byteBuffer.writeInt32(l)
              }
              byteBuffer.writeByte(F_EXT_TYPE_NAME)
              byteBuffer.write(tpeBytes)
          }

          if(TypeUtil.isMap(cls)) {
            stateStackOfElidedType.push(CodeMap)
          }
          else
            stateStackOfElidedType.push(CodeDefault)
        }


        hints.tag.key match {
          case KEY_NULL =>
            byteBuffer.writeByte(F_NULL)
          case KEY_BOOLEAN =>
            byteBuffer.writeByte(if(picklee.asInstanceOf[Boolean]) F_TRUE else F_FALSE)
          case KEY_BYTE =>
            byteBuffer.packByte(picklee.asInstanceOf[Byte])
          case KEY_SHORT =>
            byteBuffer.packInt(picklee.asInstanceOf[Short])
          case KEY_CHAR =>
            byteBuffer.packInt(picklee.asInstanceOf[Char])
          case KEY_INT =>
            byteBuffer.packInt(picklee.asInstanceOf[Int])
          case KEY_FLOAT =>
            byteBuffer.writeByte(F_FLOAT32)
            val l = java.lang.Float.floatToIntBits(picklee.asInstanceOf[Float])
            byteBuffer.writeInt32(l)
          case KEY_DOUBLE =>
            byteBuffer.writeByte(F_FLOAT64)
            val l = java.lang.Double.doubleToLongBits(picklee.asInstanceOf[Double])
            byteBuffer.writeByte(((l >>> 56) & 0xFF).toByte)
            byteBuffer.writeByte(((l >>> 48) & 0xFF).toByte)
            byteBuffer.writeByte(((l >>> 40) & 0xFF).toByte)
            byteBuffer.writeByte(((l >>> 32) & 0xFF).toByte)
            byteBuffer.writeByte(((l >>> 24) & 0xFF).toByte)
            byteBuffer.writeByte(((l >>> 16) & 0xFF).toByte)
            byteBuffer.writeByte(((l >>> 8) & 0xFF).toByte)
            byteBuffer.writeByte((l & 0xFF).toByte)
          case KEY_LONG =>
            byteBuffer.packLong(picklee.asInstanceOf[Long])
          case KEY_SCALA_STRING | KEY_JAVA_STRING =>
            byteBuffer.packString(picklee.asInstanceOf[String])
          case KEY_ARRAY_BYTE =>
            byteBuffer.packByteArray(picklee.asInstanceOf[Array[Byte]])
          case _ =>
            if(encodeTypeName && hints.isElidedType) {
              byteBuffer.writeByte(F_FIXEXT1)
              byteBuffer.writeByte(F_EXT_ELIDED_TAG)
              byteBuffer.writeByte(0) // dummy
            }
        }
      }

      this
    }

    @inline def putField(name: String, pickler: PBuilder => Unit) = {
      pickler(this)
      this
    }

    def endEntry() : Unit = withHints { hints =>
    }

    def beginCollection(length: Int) : PBuilder = {
      trace(s"begin collection: $length, $currentHint, ${stateStackOfElidedType.top}")

      stateStackOfElidedType.top match {
        case CodeMap =>
          if(length < (1 << 4))
            byteBuffer.writeByte((F_FIXMAP_PREFIX | length).toByte)
          else if(length < (1 << 16)) {
            byteBuffer.writeByte(F_MAP16)
            byteBuffer.writeShort(length.toShort)
          }
          else {
            byteBuffer.writeByte(F_MAP32)
            byteBuffer.writeInt(length)
          }
          this
        case _ =>
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
    }

    def putElement(pickler: (PBuilder) => Unit) = {
      pickler(this)
      this
    }

    def endCollection() : Unit = {

      stateStackOfElidedType.pop()

      // do nothing
    }

    def result() = {
      MsgPackPickle(byteBuffer.result())
    }
  }


  class MsgPackPickleReader(arr:Array[Byte], val mirror:Mirror, format: MsgPackPickleFormat) extends PReader with PickleTools with Logger {
    import format._
    import MsgPackCode._

    private val in = new MsgPackByteArrayReader(arr)
    private var _lastTagRead: FastTypeTag[_] = null
    private var _lastTypeStringRead: String  = null

    private val codeStack = new scala.collection.mutable.Stack[CodeState]()


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

    def beginEntryNoTag() : String = {
      val res : Any = withHints { hints =>
        trace(f"beginEntry $hints ${in.lookahead}%02x")
        if(hints.isElidedType && nullablePrimitives.contains(hints.tag.key)) {
          val la1 = in.lookahead
          la1 match {
            case F_NULL =>
              in.readByte
              FastTypeTag.Null
            case F_FIXEXT4 =>
              in.lookahead(1) match {
                case F_EXT_OBJREF =>
                  in.readByte
                  in.readByte
                  FastTypeTag.Ref
                case _ => hints.tag
              }
            case _ => hints.tag
          }
        }
        else if(hints.isElidedType && primitives.contains(hints.tag.key)) {
          hints.tag
        }
        else {
          // Non-elided type
          val la1 = in.lookahead
          la1 match {
            case F_NULL =>
              in.readByte
              FastTypeTag.Null
            case F_EXT8 =>
              val dataSize = in.lookahead(1) & 0xFF
              in.lookahead(2) match {
                case F_EXT_TYPE_NAME =>
                  in.readByte
                  in.readByte
                  in.readByte
                  val typeBytes = in.read(dataSize)
                  val typeName = new String(typeBytes, "UTF-8")
                  typeName
              }
            case F_FIXEXT1 =>
              in.lookahead(1) match {
                case F_EXT_ELIDED_TAG =>
                  in.readByte; in.readByte
                  FastTypeTag.Ref
                case _ =>
                  // TODO
                  ""
              }
            case F_FIXEXT4 =>
              in.lookahead(1) match {
                case F_EXT_OBJREF =>
                  ""
              }
            case l if (l & 0xF0).toByte == F_FIXMAP_PREFIX =>
              // TODO generate FastTypeTag
              codeStack.push(CodeMap)
              "Map[Any,Any]"
            case F_MAP16 =>
              codeStack.push(CodeMap)
              "Map[Any,Any]"
            case l if (~l & 0x80) != 0 =>
              KEY_INT
            case F_INT8 | F_INT16 | F_INT32 | F_UINT8 | F_UINT16 | F_UINT32 =>
              KEY_INT
            case F_INT64 | F_UINT64 =>
              KEY_LONG
            case F_TRUE | F_FALSE =>
              KEY_BOOLEAN
            case _ =>
              debug(f"la1: $la1%02x")
              ""
          }


        }
      }

      if (res.isInstanceOf[String]) {
        _lastTagRead = null
        _lastTypeStringRead = res.asInstanceOf[String]
        _lastTypeStringRead
      } else {
        _lastTagRead = res.asInstanceOf[FastTypeTag[_]]
        _lastTagRead.key
      }
    }


    def atPrimitive = primitives.contains(lastTagRead.key)

    def readPrimitive() : Any = {
      val key = lastTagRead.key
      val res = key match {
        case KEY_NULL => null
        case KEY_REF =>
          null // TODO
        case KEY_BYTE =>
          in.decodeInt.toByte
        case KEY_INT =>
          in.decodeInt
        case KEY_SHORT =>
          in.decodeInt.toShort
        case KEY_LONG =>
          in.decodeLong
        case KEY_SCALA_STRING | KEY_JAVA_STRING =>
          in.decodeString
        case KEY_BOOLEAN =>
          in.decodeBoolean
        case KEY_FLOAT =>
          in.decodeFloat
        case KEY_DOUBLE =>
          in.decodeDouble
        case KEY_CHAR =>
          in.decodeInt.toChar
      }
      trace(s"readPrimitive[$key] $res")
      res
    }

    def atObject = !atPrimitive

    def readField(name: String) : MsgPackPickleReader = this

    def endEntry() : Unit = {

    }

    def beginCollection() : PReader = {
      trace("begin collection")

      this
    }

    private def invalidCode(code:Byte, message:String) = new IllegalStateException(f"invalid code [$code%02x]: $message")

    /**
     * Read the length of collection
     */
    def readLength() : Int = {
      val c = in.readByte
      val len = c match {
        case l if (l & 0xF0).toByte == F_ARRAY_PREFIX =>
          l & 0x0F
        case l if (l & 0xF0).toByte == F_FIXMAP_PREFIX =>
          l & 0x0F
        case F_ARRAY16 =>
          in.readInt16
        case F_ARRAY32 =>
          in.readInt32
        case F_MAP16 =>
          in.readInt16
        case F_MAP32 =>
          in.readInt32
        case l =>
          throw invalidCode(c, "unknown collection type")
      }
      trace(s"readLength: $len")
      len
    }

    def readElement() : PReader = this

    def endCollection() : Unit = {
      if(!codeStack.isEmpty)
        codeStack.pop()
    }
  }

  object MsgPackCode {

    val F_EXT_OBJREF : Byte = 1.toByte
    val F_EXT_ELIDED_TAG : Byte = 2.toByte
    val F_EXT_TYPE_NAME : Byte = 3.toByte

    val F_FIXMAP_PREFIX = 0x80.toByte // 1000xxxx
    val F_ARRAY_PREFIX : Byte = 0x90.toByte // 1001xxxx
    val F_FIXSTR_PREFIX : Byte = 0xA0.toByte // 101xxxxx

    val F_NULL : Byte = 0xC0.toByte
    val F_NEVERUSED : Byte = 0xC1.toByte
    val F_FALSE = 0xC2.toByte
    val F_TRUE = 0xC3.toByte

    val F_BIN8 = 0xC4.toByte
    val F_BIN16 = 0xC5.toByte
    val F_BIN32 = 0xC6.toByte

    val F_EXT8 = 0xC7.toByte
    val F_EXT16 = 0xC8.toByte
    val F_EXT32 = 0xC9.toByte

    val F_FLOAT32 : Byte = 0xCA.toByte
    val F_FLOAT64 : Byte = 0xCB.toByte

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

    val F_STR8 : Byte = 0xD9.toByte
    val F_STR16 : Byte = 0xDA.toByte
    val F_STR32 : Byte = 0xDB.toByte

    val F_ARRAY16 : Byte = 0xDC.toByte
    val F_ARRAY32 : Byte = 0xDD.toByte
    val F_MAP16 : Byte = 0xDE.toByte
    val F_MAP32 : Byte = 0xDF.toByte

    val F_NEG_FIXINT_PREFIX : Byte = 0xE0.toByte

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
    val nullablePrimitives = Set(KEY_NULL, KEY_SCALA_STRING, KEY_JAVA_STRING, KEY_ARRAY_BYTE, KEY_ARRAY_SHORT, KEY_ARRAY_CHAR, KEY_ARRAY_INT, KEY_ARRAY_LONG, KEY_ARRAY_BOOLEAN, KEY_ARRAY_FLOAT, KEY_ARRAY_DOUBLE)

    type PickleType = MsgPackPickle
    type OutputType = MsgPackWriter

    def createReader(pickle: PickleType, mirror: Mirror) = new MsgPackPickleReader(pickle.value, mirror, this)

    def createBuilder() = new MsgPackPickleBuilder(this, null)
    def createBuilder(out: MsgPackWriter) = new MsgPackPickleBuilder(this, out)
  }


}
