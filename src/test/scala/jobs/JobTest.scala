package jobs

import mason.spark.configs.MainJobConfig
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class JobTest extends AnyFunSuite with BeforeAndAfter {

  test("Main Job Config test") {
    val zero = MainJobConfig()
    def parse(args: Array[String]) = {
      MainJobConfig.parser.parse(args, zero)
    }

    assert(parse(Array()) === None)
    assert(parse(Array("--job")).map(_.job) === None)
    assert(parse(Array("--job", "bad")).map(_.job) === Some("bad"))
    assert(parse(Array("--job", "merge")).map(_.job) === Some("merge"))

  }

}
