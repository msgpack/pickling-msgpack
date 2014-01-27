//--------------------------------------
//
// PicklingSpec.scala
// Since: 2014/01/27 21:20
//
//--------------------------------------

package msgpack.pickling

import org.scalatest._
import xerial.core.log.Logger
import xerial.core.util.Timer

/**
 * @author Taro L. Saito
 */
trait PicklingSpec extends WordSpec with Matchers with GivenWhenThen with OptionValues with BeforeAndAfter with Timer with Logger
