package mason.spark.jobs

import mason.spark.configs.JobConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{expr, input_file_name, max, size, udf}

class MergeJob extends SparkJob {

  def run(config: JobConfig) = {

    val INPUT_PATH = "s3a://lake-working-copy-feb-20-2020/logistics-bi-data-publisher/prod/shipment"
    val OUTPUT_PATH = "s3a://lake-working-copy-feb-20-2020/merged/"
    val EXTRACT_FILE_PATHS = true
    val REPARTITION_STRATEGY = "default"
    val ACCESS_KEY = "***REMOVED***"
    val SECRET_KEY = "***REMOVED***"

    val spark = {
      SparkSession.builder()
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .getOrCreate()
    }
    import spark.implicits._

    val df = spark.read.option("mergeSchema", "true").parquet(INPUT_PATH)
    df.count()
    df.printSchema

    def extractPath(basename: String, path: String): Array[String] = {
      val r = s"${INPUT_PATH}"
      return s"${path}"
        .replaceFirst(r, "")
        .stripPrefix("/")
        .split("/", -1)
        .dropRight(1)
    }
    val extractPathUDF = udf[Array[String], String](extractPath(INPUT_PATH, _))

    val explodedDF = if (EXTRACT_FILE_PATHS) {
      val withFileDF = df.withColumn("filename", input_file_name())
      withFileDF.select("filename").show(3)
      val extractedDF = withFileDF.withColumn("extracted_path", extractPathUDF($"filename"))
      extractedDF.select("extracted_path").show(5, false)

      //  Get maximum value of all the arrays so you can programatically add the columns
      val path_length: DataFrame = extractedDF.select(size($"extracted_path").as("path_length"))
      val maximum: Int = path_length.agg(max($"path_length")).collect().head.getInt(0)
      val column_ids: List[Int] = (0 to maximum - 1).toList
      def add_column(d: DataFrame, i: Int): DataFrame = {
        d.withColumn(s"dir_${i}", expr(s"extracted_path[${i}]"))
      }
      val explodedDF = column_ids.foldLeft(extractedDF)(add_column(_,_))
      explodedDF.show(3)

      explodedDF
    } else {
      df
    }

    explodedDF.write.mode(SaveMode.Overwrite).parquet(OUTPUT_PATH)

  }


}
