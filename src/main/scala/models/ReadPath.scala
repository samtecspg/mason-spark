package models

import org.apache.spark.sql.{DataFrame, DataFrameReader}

object ReadPath {

  def from_format(input_format: String, input_path: String, reader: DataFrameReader, read_headers: Boolean): Either[Exception, DataFrame] = {
    val ip = if (input_path.endsWith("/")) { input_path + "*"} else { input_path }

    input_format match {
      case "parquet" => Right(reader.parquet(ip))
      case "text" | "text-csv" | "csv" => Right(reader.option("header", read_headers.toString()).csv(ip))
      case "json" => Right(reader.option("multiline", true).json(ip))
      case "jsonl" => Right(reader.json(ip))
      case _: String => Left(new Exception(f"Unsupported input format: ${ip}"))
    }
  }
}

