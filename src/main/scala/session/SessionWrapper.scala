package com.example
package session

import org.apache.spark.sql.SparkSession

trait SessionWrapper {
  implicit lazy val spark: SparkSession =
    SparkSession.builder()
      .appName("Flight Analyzer")
      .getOrCreate()
}