package mason.spark.configs

import scopt.OptionParser

object PreviewConfig {
  val zero = PreviewConfig()
  val parser = new OptionParser[PreviewConfig]("mason-spark") {
    head("mason-spark", "0.1")
//    opt[String]('i', "input_path")
//      .required()
//      .valueName("<input_path>")
//      .action((x,c) => c.copy(input_path = x))
  }
}

case class PreviewConfig() extends JobConfig
