package mason.spark.models

import org.apache.spark.sql.SparkSession

object InitSparkSession {

  def with_s3_support(access_key: String, secret_key: String): SparkSession = {
    val spark = {
      SparkSession.builder()
        .master("local[*]")
        .config("fs.s3a.access.key", access_key)
        .config("fs.s3a.secret.key", secret_key)
        .getOrCreate()
    }
    import spark.implicits._
    spark
  }


}
