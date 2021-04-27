package mason.spark.jobs

import mason.spark.configs.PreviewConfig
import mason.spark.models.InitSparkSession
import models.ReadPath
import org.apache.spark.sql._

object PreviewJob {

  def run(config: PreviewConfig): Either[Throwable, String] = {
    val spark = InitSparkSession.with_s3_support(config.access_key, config.secret_key)
    val reader = spark.read
    val df: Either[Throwable, DataFrame]  = ReadPath.from_format(config.input_format, config.input_path, reader, config.read_headers)

    df.map{d =>
      d.limit(config.limit).write.mode(SaveMode.Overwrite).parquet(config.output_path)
      s"Succesfully saved parquet to ${config.output_path}"
    }
  }
}
