package models

import org.apache.spark.sql.{DataFrame, DataFrameReader}

import scala.util.{Either, Try}

object ReadPath {

  def from_format(input_format: String, input_path: String, reader: DataFrameReader, read_headers: Boolean): Either[Throwable, DataFrame] = {
    val ip = if (input_path.endsWith("/")) { input_path + "*"} else { input_path }

    input_format match {
      case "parquet" => Try(reader.parquet(ip)).toEither
      case "text" | "text-csv" | "csv" => Try(reader.option("header", read_headers.toString()).csv(ip)).toEither
      case "json" => Try(reader.option("multiline", true).json(ip)).toEither
      case "jsonl" => Try(reader.json(ip)).toEither
      case _: String => Left(new Exception(f"Unsupported input format: ${ip}"))
    }
  }
}

