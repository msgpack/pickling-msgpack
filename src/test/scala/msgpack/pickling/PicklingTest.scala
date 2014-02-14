//--------------------------------------
//
// PicklingTest.scala
// Since: 2014/01/27 21:23
//
//--------------------------------------

package msgpack.pickling

object PicklingTest {

  case class Person(id:Int, name:String)

}

import PicklingTest._
import scala.pickling._
import scala.reflect.ClassTag


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



    "serialize various types in msgpack format" in {

      import msgpack._
      def check(v:Any) {
        val encoded = v.pickle
        val decoded : Any = encoded.unpickle[Any]
        decoded shouldBe (v)
      }

      check(1)
      check("hello world")
      check(true)
      check(false)
    }


  }

}