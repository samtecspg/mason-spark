package mason.spark.jobs

import configs.FormatConfig

import mason.spark.configs.MergeConfig
import mason.spark.models.ExtractPath
import org.apache.spark.sql._
import java.nio.file.Paths

import org.apache.spark.sql.types.{BooleanType, DecimalType, DoubleType, FloatType, StringType, StructField, StructType}

object FormatJob {

  def run(conf: FormatConfig) = {

  //    spec = {
  //      "input_paths": ["s3://mason-sample-data/tests/in/csv/sample2.csv"],
  //      "input_format": "csv",
  //      "output_format": "csv",
  //      "output_path": "s3://mason-sample-data/tests/out/csv/",
  //      "filter_columns": [],
  //      "partition_columns": [],
  //      "line_terminator": "\r\n",
  //      "partitions": "3"
  //    }


    //    //TODO: Move this into util class
  //    val input_path = if (conf.input_path.endsWith("/")) {
  //      conf.input_path + "*"
  //    } else {
  //      conf.input_path
  //    }
  //
  //    val schema = StructType(List(
  //      StructField("widget",StringType,true),
  //      StructField("price", DoubleType, true),
  //      StructField("manufacturer",StringType,true),
  //      StructField("in_stock", BooleanType,true)
  //    ))
  //
  //    val schema2 = StructType(List(
  //      StructField("widget",StringType,true),
  //      StructField("price",FloatType,true),
  //      StructField("manufacturer",StringType,true),
  //      StructField("in_stock", BooleanType,true)
  //    ))
  //
  //    val spark = {
  //      SparkSession.builder()
  //        .master("local[*]")
  //        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
  //        .config("spark.hadoop.fs.s3a.access.key", conf.access_key)
  //        .config("spark.hadoop.fs.s3a.secret.key", conf.secret_key)
  //        .getOrCreate()
  //    }
  //
  //    val reader = spark.read.option("mergeSchema", "true")
  //
  //    import spark.implicits._
  //    val df = conf.input_format match {
  //      case "parquet" => reader.parquet(input_path).withColumn("price_double", $"price".cast("double")).select("price_double")
  //      case "text" => reader.option("header", conf.read_headers.toString()).csv(input_path)
  //      case "json" => reader.option("multiline", true).json(input_path)
  //      case "jsonl" => reader.json(input_path)
  //    }
  //
  //    val abs_input_path = Paths.get(conf.input_path).toUri().toString()
  //
  //    val explodedDF = if (conf.extract_file_path) {
  //      ExtractPath.extract(df, abs_input_path)
  //    } else {
  //      df
  //    }
  //
  //    explodedDF.write.mode(SaveMode.Overwrite).parquet(conf.output_path)


  }


}
