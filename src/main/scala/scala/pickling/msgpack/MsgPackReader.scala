//--------------------------------------
//
// MsgPackReader.scala
// Since: 2014/02/10 14:59
//
//--------------------------------------

package scala.pickling.msgpack

import xerial.core.log.Logger

/**
 * @author Taro L. Saito
 */
trait MsgPackReader {
  def readByte : Byte
  def read(len:Int) : Array[Byte]
  def lookahead : Byte
  def lookahead(k:Int) : Byte
  def decodeInt : Int
}


class MsgPackByteArrayReader(arr:Array[Byte]) extends MsgPackReader with Logger {
  import MsgPackCode._

  private var pos = 0

  def read(len:Int) : Array[Byte] = {
    val slice = arr.slice(pos, pos+len)
    pos += len
    slice
  }

  def readByte : Byte = {
    val v = arr(pos)
    pos += 1
    v
  }

  def lookahead = lookahead(0)
  def lookahead(k:Int) = arr(pos+k)

  private def invalidCode(c:Byte, message:String) = new IllegalStateException(f"[$c%02x] $message")

  def decodeFloat : Float = {
    val c = arr(pos)
    pos += 1
    if(c != F_FLOAT32)
      invalidCode(c, "not a float")
    val v = java.lang.Float.intBitsToFloat(
      ((arr(pos) & 0xFF) << 24) |
        ((arr(pos+1) & 0xFF) << 16) |
        ((arr(pos+2) & 0xFF) << 8) |
        (arr(pos+3) & 0xFF))
    pos += 4
    v
  }

  def decodeDouble : Double = {
    val c = arr(pos)
    pos += 1
    if(c != F_FLOAT64)
      invalidCode(c, "not a double")
    val v = java.lang.Double.longBitsToDouble(
      (((arr(pos) & 0xFF).toLong << 56) |
        ((arr(pos+1) & 0xFF).toLong << 48) |
        ((arr(pos+2) & 0xFF).toLong << 40) |
        ((arr(pos+3) & 0xFF).toLong << 32) |
        ((arr(pos+4) & 0xFF).toLong << 24) |
        ((arr(pos+5) & 0xFF).toLong << 16) |
        ((arr(pos+6) & 0xFF).toLong << 8) |
        (arr(pos+7).toLong & 0xFF)))
    pos += 8
    v
  }

  def decodeBoolean : Boolean = {
    val c = arr(pos)
    pos += 1
    c match {
      case F_TRUE =>
        true
      case F_FALSE =>
        false
      case _ =>
        throw invalidCode(c, "not a boolean")
    }
  }

  def decodeString : String = {
    val prefix = arr(pos)
    pos += 1
    val strLen : Int = prefix match {
      case l if (l & 0xE0).toByte == F_FIXSTR_PREFIX =>
        val len = l & 0x1F
        len
      case F_STR8 =>
        val len = arr(pos)
        pos += 1
        len
      case F_STR16 =>
        val len = ((arr(pos) << 8) & 0xFF) | (arr(pos+1) & 0xFF)
        pos += 2
        len
      case F_STR32 =>
        val len =((arr(pos) << 24) & 0xFF) |
            ((arr(pos+1) << 16) & 0xFF) |
            ((arr(pos+2) << 8) & 0xFF) |
            (arr(pos+3) & 0xFF)
        pos += 4
        len
      case _ =>
        throw invalidCode(prefix, "Not a string prefix")
    }
    new String(read(strLen), "UTF-8")
  }

  def decodeInt : Int = {
    val prefix = arr(pos)
    prefix match {
      case l if l < 127 =>
        pos += 1
        prefix
      case F_UINT8 =>
        val v = arr(pos + 1).toInt
        pos += 2
        v
      case F_UINT16 =>
        val v = (((arr(pos + 1) & 0xFF) << 8) | (arr(pos+2) & 0xFF)).toInt
        pos += 3
        v
      case F_UINT32 =>
        val v =
          (((arr(pos+1) & 0xFF) << 24)
            | ((arr(pos+2) & 0xFF) << 16)
            | ((arr(pos+3) & 0xFF) << 8)
            | (arr(pos+4) & 0xFF)).toInt
        pos += 5
        v
      case F_UINT64 =>
        throw new IllegalStateException("Cannot decode UINT64")
//        val v =
//          (((arr(pos+1) & 0xFF) << 54)
//            | ((arr(pos+2) & 0xFF) << 48)
//            | ((arr(pos+3) & 0xFF) << 40)
//            | ((arr(pos+4) & 0xFF) << 32)
//            | ((arr(pos+5) & 0xFF) << 24)
//            | ((arr(pos+6) & 0xFF) << 16)
//            | ((arr(pos+7) & 0xFF) << 8)
//            | (arr(pos+8) & 0xFF)).toInt
//        pos += 9
//        v
      case F_INT8 =>
        val v = arr(pos + 1)
        pos += 2
        if(v < 0) v & (~0 << 8) else v
      case F_INT16 =>
        val p = arr(pos+1)
        val v = (((arr(pos + 1) & 0xFF) << 8) | (arr(pos+2) & 0xFF)).toInt
        pos += 3
        if(p < 0) v & (~0 << 16) else v
      case F_INT32 =>
        val v =
          (((arr(pos+1) & 0xFF) << 24)
            | ((arr(pos+2) & 0xFF) << 16)
            | ((arr(pos+3) & 0xFF) << 8)
            | (arr(pos+4) & 0xFF)).toInt
        pos += 5
        v
      case F_INT64 =>
        throw new IllegalStateException("Cannot decode INT64")
//        val v =
//          (((arr(pos+1) & 0xFF) << 54)
//            | ((arr(pos+2) & 0xFF) << 48)
//            | ((arr(pos+3) & 0xFF) << 40)
//            | ((arr(pos+4) & 0xFF) << 32)
//            | ((arr(pos+5) & 0xFF) << 24)
//            | ((arr(pos+6) & 0xFF) << 16)
//            | ((arr(pos+7) & 0xFF) << 8)
//            | (arr(pos+8) & 0xFF)).toInt
//        pos += 9
//        v
      case _ =>
        throw new IllegalStateException("not an integer")
    }

  }

  def decodeLong : Long = {
    val prefix = arr(pos)
    pos += 1
    prefix match {
      case l if l > 0 =>
        prefix
      case F_UINT8 =>
        val v = arr(pos).asInstanceOf[Long]
        pos += 1
        v
      case F_UINT16 =>
        val v = (((arr(pos).asInstanceOf[Long] & 0xFF) << 8)
          | (arr(pos+1).asInstanceOf[Long] & 0xFF))
        pos += 2
        v
      case F_UINT32 =>
        val v =
          (((arr(pos).asInstanceOf[Long] & 0xFF) << 24)
            | ((arr(pos+1).asInstanceOf[Long] & 0xFF) << 16)
            | ((arr(pos+2).asInstanceOf[Long] & 0xFF) << 8)
            | (arr(pos+3).asInstanceOf[Long] & 0xFF))
        pos += 4
        v
      case F_UINT64 =>
        val v =
          (((arr(pos) & 0xFF).toLong << 54)
            | ((arr(pos+1).asInstanceOf[Long] & 0xFF) << 48)
            | ((arr(pos+2).asInstanceOf[Long] & 0xFF) << 40)
            | ((arr(pos+3).asInstanceOf[Long] & 0xFF) << 32)
            | ((arr(pos+4).asInstanceOf[Long] & 0xFF) << 24)
            | ((arr(pos+5).asInstanceOf[Long] & 0xFF) << 16)
            | ((arr(pos+6).asInstanceOf[Long] & 0xFF) << 8)
            | (arr(pos+7).asInstanceOf[Long] & 0xFF))
        pos += 8
        v
      case F_INT8 =>
        val v = arr(pos)
        pos += 1
        if(v < 0) v & (~0 << 8) else v
      case F_INT16 =>
        val p = arr(pos)
        val v = (((arr(pos) & 0xFF) << 8) | (arr(pos+1) & 0xFF)).toInt
        pos += 2
        if(p < 0) v & (~0 << 16) else v
      case F_INT32 =>
        val v =
          (((arr(pos).asInstanceOf[Long] & 0xFF) << 24)
            | ((arr(pos+1).asInstanceOf[Long] & 0xFF) << 16)
            | ((arr(pos+2).asInstanceOf[Long] & 0xFF) << 8)
            | (arr(pos+3).asInstanceOf[Long] & 0xFF))
        pos += 4
        v
      case F_INT64 =>
        val v =
          (((arr(pos).asInstanceOf[Long] & 0xFF) << 54)
            | ((arr(pos+1).asInstanceOf[Long] & 0xFF) << 48)
            | ((arr(pos+2).asInstanceOf[Long] & 0xFF) << 40)
            | ((arr(pos+3).asInstanceOf[Long] & 0xFF) << 32)
            | ((arr(pos+4).asInstanceOf[Long] & 0xFF) << 24)
            | ((arr(pos+5).asInstanceOf[Long] & 0xFF) << 16)
            | ((arr(pos+6).asInstanceOf[Long] & 0xFF) << 8)
            | (arr(pos+7).asInstanceOf[Long] & 0xFF))
        pos += 8
        v
      case _ =>
        throw new IllegalStateException("not a long")
    }

  }

}