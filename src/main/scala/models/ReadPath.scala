package models

import org.apache.spark.sql.{DataFrame, DataFrameReader}

object ReadPath {

  def from_format(input_format: String, input_path: String, reader: DataFrameReader, read_headers: Boolean): Either[Exception, DataFrame] = input_format match {
    case "parquet" => Right(reader.parquet(input_path))
    case "text" | "text-csv" | "csv" => Right(reader.option("header", read_headers.toString()).csv(input_path))
    case "json" => Right(reader.option("multiline", true).json(input_path))
    case "jsonl" => Right(reader.json(input_path))
    case _: String => Left(new Exception(f"Unsupported input format: ${input_format}"))
  }

}
