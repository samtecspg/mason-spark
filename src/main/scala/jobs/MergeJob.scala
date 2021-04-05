package mason.spark.jobs

import java.nio.file.Paths
import mason.spark.configs.MergeConfig
import mason.spark.models.ExtractPath
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession

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
        .config("fs.s3a.access.key", conf.access_key)
        .config("fs.s3a.secret.key", conf.secret_key)
        .getOrCreate()
    }

    val reader = spark.read.option("mergeSchema", "true")

    import spark.implicits._
    val df: Option[DataFrame] = conf.input_format match {
      case "parquet" => Some(reader.parquet(input_path))
      case "text" | "text-csv" | "csv" => Some(reader.option("header", conf.read_headers.toString()).csv(input_path))
      case "json" => Some(reader.option("multiline", true).json(input_path))
      case "jsonl" => Some(reader.json(input_path))
      case _: String => println(f"Unsupported input format: ${conf.input_format}"); None
    }

    val abs_input_path = Paths.get(conf.input_path).toUri().toString()

    val explodedDF = if (conf.extract_file_path) {
      df.map { d => ExtractPath.extract(d, abs_input_path) }
    } else {
      df
    }

    explodedDF.map{d => d.write.mode(SaveMode.Overwrite).parquet(conf.output_path)}

  }


}
