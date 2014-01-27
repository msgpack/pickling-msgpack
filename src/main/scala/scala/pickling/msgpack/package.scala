package scala.pickling

import scala.reflect.runtime.universe.Mirror

/**
 * @author Taro L. Saito
 */
package object msgpack {

  implicit val msgpackFormat = new MsgPackPickleFormat

}

package msgpack {

import scala.pickling.binary.BinaryPickleFormat

case class MsgPackPickle(value:Array[Byte]) extends Pickle {
    type ValueType = Array[Byte]
    type PickleFormatType = BinaryPickleFormat

    private def toHEX(b:Array[Byte]) = b.map(x => f"$x%x").mkString
    override def toString = s"""MsgPackPickle(${toHEX(value)})"""
  }

  class MsgPackPickleBuilder(format:MsgPackPickleFormat, out:EncodingOutput[Array[Byte]]) extends PBuilder with PickleTools {
    def beginEntry(picklee: Any) = ???

    def putField(name: String, pickler: (PBuilder) => Unit) = ???

    def endEntry() = ???

    def beginCollection(length: Int) = ???

    def putElement(pickler: (PBuilder) => Unit) = ???

    def endCollection() = ???

    def result() = ???
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

    type PickleType = MsgPackPickle
    type OutputType = EncodingOutput[Array[Byte]]

    def createReader(pickle: PickleType, mirror: Mirror) = new MsgPackPickleReader(pickle.value, mirror, this)

    def createBuilder() : PBuilder = new MsgPackPickleBuilder(this, null)

    def createBuilder(out: MsgPackPickleFormat#OutputType) = new MsgPackPickleBuilder(this, out)
  }


}
