package com.example
package validation

import errors.ValidationErrors

import org.apache.spark.sql.DataFrame

trait DfValidator {
  def validateColumnPresence(requiredColumns: Seq[String])(df: DataFrame): Unit = {
    val missing = requiredColumns.diff(df.columns.toSeq)

    if (missing.nonEmpty) {
      throw ValidationErrors.MissingColumnsException(
        s"Missing required columns: [${missing.mkString(", ")}]. Actual columns: [${df.columns.mkString(", ")}]"
      )
    }
  }

  def validateNotEmpty(df: DataFrame, dfName: String): Unit = {
    if (df.isEmpty) {
      throw ValidationErrors.EmptyDataException(s"DataFrame [$dfName] is empty")
    }
  }
}
