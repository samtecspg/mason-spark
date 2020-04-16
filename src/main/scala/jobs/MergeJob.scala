package mason.spark.jobs

import mason.spark.configs.MergeConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{expr, input_file_name, max, size, udf}

object MergeJob {

  def run(conf: MergeConfig, spark: SparkSession) = {

    val spark = conf.spark
    import spark.implicits._

    //TODO: Move this into util class
    val input_path = if (conf.input_path.endsWith("/")) {
      conf.input_path + "*"
    } else {
      conf.input_path
    }

    val reader = spark.read.option("mergeSchema", "true").option("header", conf.read_headers.toString())

    //TODO:  Remove try catch
    val df: DataFrame =
      try {
        reader.parquet(input_path)
      } catch {
        case e: org.apache.spark.SparkException => {
          reader.csv(input_path)
        }
      }

    df.count()
    df.printSchema

    def extractPath(basename: String, path: String): Array[String] = {
      val r = s"${conf.input_path}"
      return s"${path}"
        .replaceFirst(r, "")
        .stripPrefix("/")
        .split("/", -1)
        .dropRight(1)
    }
    val extractPathUDF = udf[Array[String], String](extractPath(conf.input_path, _))

    val explodedDF = if (conf.extract_file_path) {
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

    explodedDF.write.mode(SaveMode.Overwrite).parquet(conf.output_path)

  }


}
