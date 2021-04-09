package mason.spark.jobs

import mason.spark.configs.SummaryConfig
import mason.spark.models.InitSparkSession
import models.ReadPath
import org.apache.spark.sql.{DataFrame, SaveMode}

object SummaryJob {

  def run(config: SummaryConfig): Either[Exception, String] = {
    val spark = InitSparkSession.with_s3_support(config.access_key, config.secret_key)
    val reader = spark.read
    val df: Either[Exception, DataFrame]  = ReadPath.from_format(config.input_format, config.input_path, reader, config.read_headers)

    def totalQuery(columns: Array[String]) = {
      s"""SELECT
        '*' AS column,
        COUNT(*) AS count,
        COUNT(DISTINCT(*)) AS distinct_count,
        (SELECT COUNT(*) from df WHERE ${columns.map(c => s"${c} IS NOT NULL").mkString(" AND ")}) as non_null_count,
        (SELECT COUNT(*) from df WHERE ${columns.map(c => s"${c} IS NULL").mkString(" AND ")}) as null_count,
        NULL as max,
        NULL as min
      FROM df
      """.stripMargin
    }

    def columnQuery(column: String) : String = {
      s"""SELECT
          '${column}' AS column,
          COUNT(${column}) AS count,
          COUNT(DISTINCT(${column})) AS distinct_count,
          (SELECT COUNT(*) from df WHERE ${column} IS NOT NULL) as non_null_count,
          (SELECT COUNT(*) from df WHERE ${column} IS NULL) as null_count,
          MAX(${column}) as max,
          MIN(${column}) as min
      FROM df
      """.stripMargin
    }

    df.map{d =>
      d.createOrReplaceTempView("df")
      val columns = d.schema.fields.map(f => f.name)
      val summaryQuery = (Array(totalQuery(columns)) ++ columns.map(c => columnQuery(c))).mkString("\nUNION ALL\n")
      val summaryDF = spark.sql(summaryQuery)
      summaryDF.write.mode(SaveMode.Overwrite).parquet(config.output_path)
      s"Succesfully saved parquet to ${config.output_path}"
    }
  }
}
