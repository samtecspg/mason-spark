package mason.spark.configs
import scopt.OptionParser

object FormatConfig {
  val zero: FormatConfig = FormatConfig()
  val parser: OptionParser[FormatConfig] = new OptionParser[FormatConfig]("mason-spark") {
    //    opt[String]('i', "input_path")
    //      .required()
    //      .valueName("<input_path>")
    //      .action((x,c) => c.copy(input_path = x))
  }
}

case class FormatConfig() extends JobConfig



