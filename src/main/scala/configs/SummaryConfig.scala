package mason.spark.configs

import scopt.OptionParser

object SummaryConfig {
  val zero = SummaryConfig("", "", "", false, "", "")
  val parser = new OptionParser[SummaryConfig]("mason-spark") {
    opt[String]('i', "input_path")
      .required()
      .valueName("<input_path>")
      .action((x,c) => c.copy(input_path = x))
    opt[String]('i', "input_format")
      .required()
      .valueName("<input_format>")
      .action((x,c) => c.copy(input_format = x))
    opt[String]('i', "output_path")
      .required()
      .valueName("<output_path>")
      .action((x,c) => c.copy(output_path = x))
    opt[Boolean]('i', "read_headers")
      .optional()
      .valueName("<read_headers>")
      .action((x,c) => c.copy(read_headers = x))
    opt[String]('i', "access_key")
      .required()
      .valueName("<access_key>")
      .action((x,c) => c.copy(access_key = x))
    opt[String]('i', "secret_key")
      .required()
      .valueName("<secret_key>")
      .action((x,c) => c.copy(secret_key = x))
  }
}

case class SummaryConfig(
  input_path: String,
  input_format: String,
  output_path: String,
  read_headers: Boolean,
  access_key: String,
  secret_key: String
) extends JobConfig



