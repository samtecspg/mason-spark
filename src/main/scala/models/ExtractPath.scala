package mason.spark.models

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{expr, input_file_name, max, size, udf}

object ExtractPath extends Serializable {

  def extract(df: DataFrame, input_path: String): DataFrame = {
    import df.sparkSession.implicits._

    val withFileDF = df.withColumn("filename", input_file_name())
    def extractPath(basename: String, path: String): Array[String] = {
      val r = s"${input_path}"
      s"${path}"
        .replaceFirst(r, "")
        .stripPrefix("/")
        .split("/", -1)
        .dropRight(1)
    }
    val extractPathUDF = udf[Array[String], String](extractPath(input_path, _))
    val extractedDF = withFileDF.withColumn("extracted_path", extractPathUDF($"filename"))

    //  Get maximum value of all the arrays so you can programatically add the columns
    val path_length: DataFrame = extractedDF.select(size($"extracted_path").as("path_length"))
    val maximum: Int = path_length.agg(max($"path_length")).collect().head.getInt(0)
    val column_ids: List[Int] = (0 to maximum - 1).toList
    def add_column(d: DataFrame, i: Int): DataFrame = {
      d.withColumn(s"dir_${i}", expr(s"extracted_path[${i}]"))
    }
    val explodedDF = column_ids.foldLeft(extractedDF)(add_column(_,_))
    explodedDF
  }

}
