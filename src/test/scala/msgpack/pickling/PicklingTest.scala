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
import scala.language.existentials


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

    def check[A : FastTypeTag : SPickler : Unpickler](v:A) = {
      import msgpack._
      debug(s"pickling $v")
      val encoded = v.pickle
      debug(s"unpickling $encoded")
      val decoded = encoded.unpickle[A]
      debug(s"decoded $decoded")
      decoded shouldBe (v)
    }


    "serialize primitive types in msgpack format" taggedAs(Tag("primitive")) in {
      import msgpack._

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

    "serialize Array types" taggedAs(Tag("array")) in {
      import msgpack._

      val in = Array(Person(1, "leo"), Person(2, "yui"))
      check(in)
    }

    "serialize Seq types" in {
      import msgpack._
      check(Seq(Person(1, "leo"), Person(2, "yui")))
      check(IndexedSeq(Person(1, "leo"), Person(2, "yui")))
      check(Vector(Person(1, "leo"), Person(2, "yui")))
      check(Set(Person(1, "leo"), Person(2, "yui")))
    }




  }

}