package com.example
package readers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object CsvReader {
  case class Config(
    file: String,
    schema: StructType,
    separator: String = ",",
    hasHeader: Boolean = true
  )
}

class CsvReader(config: CsvReader.Config)(implicit spark: SparkSession) extends DataFrameReader {
  override def read(): DataFrame = {
    val df = spark.read
      .option("header", config.hasHeader.toString)
      .option("sep", config.separator)
      .option("mode", "FAILFAST")
      .schema(config.schema)
      .csv(config.file)

    df
  }
}