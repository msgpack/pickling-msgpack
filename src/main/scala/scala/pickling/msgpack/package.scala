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

case class MsgPackPickle(value:Array[Byte]) extends Pickle {
    type ValueType = Array[Byte]
    type PickleFormatType = BinaryPickleFormat

    private def toHEX(b:Array[Byte]) = b.map(x => f"$x%x").mkString
    override def toString = s"""MsgPackPickle(${toHEX(value)})"""
  }


  trait MsgPackOutput extends Output[Array[Byte]] {

    def encodeByteTo(pos:Int, b:Byte)

  }

  class MsgPackOutputArray(size:Int) extends MsgPackOutput {

    private val arr = new Array[Byte](size)

    def result() = arr

    def put(obj: Array[Byte]) = ???

    def encodeByteTo(pos: Int, b: Byte) =


  }
  class MsgPackOutputBuffer extends MsgPackOutput {
    private val b = ArrayBuffer[Byte]()

    def result() = ???

    def put(obj: Array[Byte]) = ???
  }



  class MsgPackPickleBuilder(format:MsgPackPickleFormat, out:MsgPackOutput) extends PBuilder with PickleTools {
    import format._

    private var byteBuffer: MsgPackOutput = out

    private var pos = 0

    @inline private[this] def mkByteBuffer(knownSize: Int): Unit =
      if (byteBuffer == null) {
        byteBuffer = if (knownSize != -1) new MsgPackOutputArray(knownSize) else new MsgPackOutputBuffer
      }

    def beginEntry(picklee: Any) = withHints { hints =>
      mkByteBuffer(hints.knownSize)

      if(picklee == null) {
        byteBuffer.encodeByteTo(pos, F_NULL)
        pos = pos + 1
      }
      else if (hints.oid != -1) {
        val oid = hints.oid
        //if(oid <= 0)



      } else {
        if(!hints.isElidedType) {
          val tpeBytes = hints.tag.key.getBytes("UTF-8")
          byteBuffer.encode


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




    type PickleType = MsgPackPickle
    type OutputType = MsgPackOutput

    def createReader(pickle: PickleType, mirror: Mirror) = new MsgPackPickleReader(pickle.value, mirror, this)

    def createBuilder() : PBuilder = new MsgPackPickleBuilder(this, null)

    def createBuilder(out: MsgPackPickleFormat#OutputType) = new MsgPackPickleBuilder(this, out)
  }


}
