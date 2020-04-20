package mason.spark.jobs

import mason.spark.configs.MergeConfig
import mason.spark.models.ExtractPath
import org.apache.spark.sql._
import java.nio.file.Paths

object MergeJob {

  def run(conf: MergeConfig) = {

    //TODO: Move this into util class
    val input_path = if (conf.input_path.endsWith("/")) {
      conf.input_path + "*"
    } else {
      conf.input_path
    }

    val spark = {
      SparkSession.builder()
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", conf.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", conf.secret_key)
        .getOrCreate()
    }

    val reader = spark.read.option("mergeSchema", "true").option("header", conf.read_headers.toString())

    // TODO: Expand information being passed from metastore
    val df = conf.input_format match {
      case "parquet" => reader.parquet(input_path)
      case "text-csv" => reader.csv(input_path)
      case "json" => reader.option("multiline", true).json(input_path)
      case "jsonl" => reader.json(input_path)
    }

    val abs_input_path = Paths.get(conf.input_path).toUri().toString()

    val explodedDF = if (conf.extract_file_path) {
      ExtractPath.extract(df, abs_input_path)
    } else {
      df
    }

    explodedDF.write.mode(SaveMode.Overwrite).parquet(conf.output_path)


  }


}
