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


/**
 * @author Taro L. Saito
 */
class PicklingTest extends PicklingSpec {

  "Pickling" should {

    "serialize objects" in {

      val p = Person(1, "leo")
      debug(p)

      val pckl = p.pickle
      val pp = pckl.unpickle[Person]
      debug(pp)
      
      p shouldBe pp
    }

  }

}