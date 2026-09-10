package com.example
package readers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait DataFrameReader {
  def read(file: String, schema: StructType): DataFrame
}