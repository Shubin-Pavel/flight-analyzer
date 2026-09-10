package com.example
package readers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

final case class CsvReaderConfig(
  separator: String = ",",
  hasHeader: Boolean = true
)

final class CsvReader(config: CsvReaderConfig = CsvReaderConfig())
                     (implicit spark: SparkSession) extends DataFrameReader {
  override def read(file: String, schema: StructType): DataFrame = spark
    .read
    .option("header", config.hasHeader.toString)
    .option("sep", config.separator)
    .option("mode", "FAILFAST")
    .schema(schema)
    .csv(file)
}