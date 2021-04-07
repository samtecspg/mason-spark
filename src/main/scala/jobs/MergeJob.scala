package mason.spark.jobs

import java.nio.file.Paths
import mason.spark.configs.MergeConfig
import mason.spark.models.{ExtractPath, InitSparkSession}
import models.ReadPath
import org.apache.spark.sql._

object MergeJob {

  def run(config: MergeConfig): Either[Exception, String] = {
    val spark = InitSparkSession.with_s3_support(config.access_key, config.secret_key)
    val reader = spark.read.option("inferSchema", true).option("mergeSchema", true)
    val df: Either[Exception, DataFrame]  = ReadPath.from_format(config.input_format, config.input_path, reader, config.read_headers)

    val abs_input_path = Paths.get(config.input_path).toUri().toString()
    val explodedDF = if (config.extract_file_path) { df.map { d => ExtractPath.extract(d, abs_input_path) }} else { df }

    explodedDF.map{d =>
      d.write.mode(SaveMode.Overwrite).parquet(config.output_path)
      s"Succesfully saved parquet to ${config.output_path}"
    }

  }

}
