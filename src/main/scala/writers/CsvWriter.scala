package com.example
package writers

import org.apache.spark.sql.{DataFrame, SaveMode}

final class CsvWriter extends Writer {
  override def write(df: DataFrame, path: String): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(path)
  }
}