package mason.spark.jobs

import java.nio.file.Paths
import mason.spark.configs.MergeConfig
import mason.spark.models.ExtractPath
import models.ReadPath
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession

object MergeJob {

  def run(config: MergeConfig): Either[Exception, String] = {

    //TODO: Move this into util class
    val input_path = if (config.input_path.endsWith("/")) {
      config.input_path + "*"
    } else {
      config.input_path
    }

    val spark = {
      SparkSession.builder()
        .master("local[*]")
        .config("fs.s3a.access.key", config.access_key)
        .config("fs.s3a.secret.key", config.secret_key)
        .getOrCreate()
    }

    import spark.implicits._
    val reader = spark.read.option("inferSchema", true).option("mergeSchema", true)

    val df: Either[Exception, DataFrame]  = ReadPath.from_format(config.input_format, input_path, reader, config.read_headers)
    val abs_input_path = Paths.get(config.input_path).toUri().toString()

    val explodedDF = if (config.extract_file_path) {
      df.map { d => ExtractPath.extract(d, abs_input_path) }
    } else {
      df
    }

    explodedDF.map{d => d.write.mode(SaveMode.Overwrite).parquet(config.output_path); s"Succesfully saved parquet to ${config.output_path}"}

  }

}
