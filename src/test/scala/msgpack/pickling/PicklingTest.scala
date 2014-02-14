//--------------------------------------
//
// PicklingTest.scala
// Since: 2014/01/27 21:23
//
//--------------------------------------

package msgpack.pickling

object PicklingTest {

  case class Person(id:Int, name:String)

  case class Prim(c1:Int, c2:Float, c3:Double, c4:Boolean, c5:Long, c6:Byte, c7:String)

}

import PicklingTest._
import scala.pickling._
import scala.reflect.ClassTag
import org.scalatest.Tag


/**
 * @author Taro L. Saito
 */
class PicklingTest extends PicklingSpec {

  def toHEX(b:Array[Byte]) = b.map(c => f"$c%x").mkString

  "Pickling" should {

    "serialize objects in JSON format" in {

      val p = Person(1, "leo")

      import json._

      val pckl = p.pickle
      debug(pckl)
      val pp = pckl.unpickle[Person]
      debug(pp)

      p shouldBe pp
    }

    "serialize objects in the default binary format" in {

      val p = Person(1, "leo")


      import binary._

      val pckl = p.pickle



      debug(toHEX(pckl.value))
      val pp = pckl.unpickle[Person]
      debug(pp)

      p shouldBe pp
    }


    "serialize objects in msgpack format" in {

      import msgpack._

      val p = Person(1, "leo")
      val pckl = p.pickle

      debug(pckl)

      val pp = pckl.unpickle[Person]
      debug(pp)

      p shouldBe pp
    }



    "serialize primitive types in msgpack format" taggedAs(Tag("primitive")) in {
      import msgpack._
      def check(v:Any) {
        debug(s"pickling $v")
        val encoded = v.pickle
        debug(s"unpickling $encoded")
        val decoded : Any = encoded.unpickle[Any]
        decoded shouldBe (v)
      }

      check(1)
      check("hello world")
      check(true)
      check(false)

      check(1.34f)
      check(0.14434)
      check('h'.toChar)

      check(34.toByte)

      // check(null)
      check(Prim(1, 43.43234f, 0.32434234234, false, 9234234L, 42.toByte, "Hello Scala Pickling"))

    }

    "should serialize Array types" taggedAs(Tag("array")) in {
      import msgpack._

      val in = Array(Person(1, "leo"), Person(2, "yui"))
      debug(in.mkString(", "))
      val p = in.pickle
      debug(p)
      val up = p.unpickle[Array[Person]]
      debug(up.mkString(", "))

      up shouldBe in
    }


  }

}