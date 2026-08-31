package com.example
package validation

import org.apache.spark.sql.DataFrame

case class MissingColumnsException(message: String) extends Exception(message)
case class EmptyDataException(message: String) extends Exception(message)

trait DfValidator {
  def validateColumnPresence(requiredColumns: Seq[String])(df: DataFrame): Unit = {
    val missing = requiredColumns.diff(df.columns.toSeq)
    if (missing.nonEmpty) {
      throw MissingColumnsException(
        s"Missing columns: [${missing.mkString(", ")}]. Actual columns: [${df.columns.mkString(", ")}]"
      )
    }
  }

  def validateNotEmpty(df: DataFrame, dfName: String): Unit = {
    if (df.isEmpty) {
      throw EmptyDataException(s"DataFrame [$dfName] is empty")
    }
  }
}
