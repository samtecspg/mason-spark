package mason.spark.configs

import mason.spark.jobs.MergeJob
import scopt.OptionParser

object MergeConfig {
  def zero = MergeConfig("", "", false, "", "", "")
}

case class MergeConfig(
  input_path: String,
  output_path: String,
  extract_file_path: Boolean = false,
  repartition_keys: String,
  //TODO: REMOVE THIS FROM MERGE CONFIG PROPER
  access_key: String,
  secret_key: String
) {

  // TODO: Replace with scopt functional parser
  val parser = new OptionParser[MergeConfig]("mason-spark") {
    head("mason-spark", "0.1")
    opt[String]('i', "input_path")
      .required()
      .valueName("<input_path>")
      .action((x,c) => c.copy(input_path = x))
    opt[String]('o', "output_path")
      .required()
      .valueName("<output_path>")
      .action((x,c) => c.copy(output_path = x))
    opt[Boolean]('p', "extract_file_paths")
      .valueName("<extract_paths>")
      .action((x,c) => c.copy(extract_file_path = x))
    opt[String]('r', "repartition_keys")
      .valueName("<repartition_keys>")
      .action((x,c) => c.copy(repartition_keys = x))
    // BIG TODO:  Wrap the metastore configuration into a seperate config and pass this along so that this is not aws specific
    opt[String]('a', "access_key")
      .valueName("<access_key>")
      .required()
      .action((x,c) => c.copy(access_key = x))
    opt[String]('s', "secret_key")
      .valueName("<secret_key>")
      .required()
      .action((x,c) => c.copy(secret_key = x))
  }

  def run() = {
    (new MergeJob).run(this)
  }

}
