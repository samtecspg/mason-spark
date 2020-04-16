package mason.spark.configs

import mason.spark.jobs.MergeJob
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object MergeConfig {
  def zero = MergeConfig("", "")
}

case class MergeConfig(
  input_path: String,
  output_path: String,
  extract_file_path: Boolean = false,
  repartition_keys: String = "",
  read_headers: Boolean = true,
  //TODO: REMOVE THIS FROM MERGE CONFIG PROPER
  access_key: String = "",
  secret_key: String = ""
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
    opt[Boolean]('h', "read_headers")
      .valueName("<repartition_keys>")
      .action((x,c) => c.copy(read_headers = x))
    // BIG TODO:  Wrap the metastore configuration into a seperate config and pass this along so that this is not aws specific
    opt[String]('a', "access_key")
      .valueName("<access_key>")
      .action((x,c) => c.copy(access_key = x))
    opt[String]('s', "secret_key")
      .valueName("<secret_key>")
      .action((x,c) => c.copy(secret_key = x))
  }

  val spark = {
    SparkSession.builder()
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.access.key", access_key)
      .config("spark.hadoop.fs.s3a.secret.key", secret_key)
      .getOrCreate()
  }

  def run() = {
    MergeJob.run(this, spark)
  }

}
