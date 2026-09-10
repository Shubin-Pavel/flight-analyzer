package com.example
package writers

import org.apache.spark.sql.DataFrame

trait Writer {
  def write(df: DataFrame, path: String): Unit
}
