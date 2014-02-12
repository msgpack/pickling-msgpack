//--------------------------------------
//
// MsgPackReader.scala
// Since: 2014/02/10 14:59
//
//--------------------------------------

package scala.pickling.msgpack

/**
 * @author Taro L. Saito
 */
trait MsgPackReader {
  def readByte : Byte
  def lookahead : Byte
  def lookahead(k:Int) : Byte
  def decodeInt : Int
}


class MsgPackByteArrayReader(arr:Array[Byte]) extends MsgPackReader {
  import MsgPackCode._

  private var pos = 0

  def readByte : Byte = {
    val v = arr(pos)
    pos += 1
    v
  }

  def lookahead = lookahead(0)
  def lookahead(k:Int) = arr(pos+k)

  def decodeInt : Int = {
    val prefix = arr(pos)
    import scala.pickling.msgpack.MsgPackPickleFormat
    prefix match {
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


}