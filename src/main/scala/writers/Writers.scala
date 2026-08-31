package com.example
package writers

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Writers {
  def writeCsv(df: DataFrame, path: String): Unit = {
    df
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(path)
  }
}
