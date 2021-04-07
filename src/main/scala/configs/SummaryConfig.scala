package mason.spark.configs

import scopt.OptionParser

object SummaryConfig {
  val zero = SummaryConfig()
  val parser = new OptionParser[SummaryConfig]("mason-spark") {
    //    opt[String]('i', "input_path")
    //      .required()
    //      .valueName("<input_path>")
    //      .action((x,c) => c.copy(input_path = x))
  }
}

case class SummaryConfig() extends JobConfig


