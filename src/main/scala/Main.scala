
package mason.spark
import java.io.File

//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.rdd.RDD
//import scopt.OptionParser

case class Config(input: String = null, output:String = null)

object Main extends App {
  println("hello Scopt")
  val parser = new scopt.OptionParser[Config]("scopt") {
    head("scopt", "3.x")
    opt[String]('f', "input") required() action { (x, c) =>
      c.copy(input = x) } text("input is the input path")
    opt[String]('o', "output") required() action { (x, c) =>
      c.copy(output = x) } text("output is the output path")
  }
  // parser.parse returns Option[C]
  parser.parse(args, Config()) map { config =>
    // do stuff
    val input = config.input
    val output = config.output
    println("input==" + input)
    println("output=" + output)
    val sc = new SparkContext()
    val rdd = sc.textFile(input)
    rdd.saveAsTextFile(output)

  } getOrElse {
    // arguments are bad, usage message will have been displayed
  }
}
