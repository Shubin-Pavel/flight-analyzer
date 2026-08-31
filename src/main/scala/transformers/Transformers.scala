package com.example
package transformers

import types._

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object Transformers {
  private def orderColumn(field: String)(implicit sort: SortDirection): Column = {
    sort match {
      case Asc  => col(field).asc
      case Desc => col(field).desc
    }
  }

  def filterByCond(cond: Column)(df: DataFrame): DataFrame =
    df
      .filter(cond)

  def joinByCond(rightDf: DataFrame, cond: Column, joinType: String)(leftDf: DataFrame): DataFrame =
    leftDf
      .join(rightDf, cond, joinType)

  def selectColumns(columns: Seq[Column])(df: DataFrame): DataFrame =
    df
      .select(columns: _*)

  def aggByGroup(columnsGroup: Seq[Column], aggGroup: Seq[Column])(df: DataFrame): DataFrame = {
    if (columnsGroup.isEmpty) {
      df
        .agg(
          aggGroup.head,
          aggGroup.tail: _*
        )
    }
    else {
      df
        .groupBy(columnsGroup: _*)
        .agg(
          aggGroup.head,
          aggGroup.tail: _*
        )
    }
  }

  def withDelayInfo(df: DataFrame): DataFrame =
    df
      .select(
        col("*"),
        (col("departure_delay") > 0).as("is_departure_delay"),
        (col("arrival_delay") > 0).as("is_arrival_delay"),
        (col("air_system_delay") > 0).as("is_air_system_delay"),
        (col("security_delay") > 0).as("is_security_delay"),
        (col("airline_delay") > 0).as("is_airline_delay"),
        (col("late_aircraft_delay") > 0).as("is_late_aircraft_delay"),
        (col("weather_delay") > 0).as("is_weather_delay"),
      )

  def withPeriodDt(df: DataFrame): DataFrame =
    df
      .withColumn("period_dt", to_date(concat_ws("-", col("year"), col("month"), col("day"))))

  def mergeDataFrames(rightDf: DataFrame, keys: Seq[String])(leftDf: DataFrame): DataFrame = {
    val aliasLeft = "left"
    val aliasRight = "right"

    val joinCondition =
      if (keys.isEmpty) lit(true)
      else keys
        .map(key => col(s"$aliasLeft.$key") === col(s"$aliasRight.$key"))
        .reduce(_ && _)

    val joinedDf = leftDf.alias(aliasLeft)
      .join(rightDf.alias(aliasRight), joinCondition, "full")
    
    val leftColumns = leftDf.columns
    val rightColumns = rightDf.columns
    val intersectColumns = leftColumns.intersect(rightColumns)
    
    val coalesceColumns = intersectColumns
      .diff(keys)
      .map(columnName => (
        coalesce(col(s"$aliasLeft.$columnName"), lit(0))
          + coalesce(col(s"$aliasRight.$columnName"), lit(0))
        ).as(columnName)
      )
    val leftUniqColumns = leftColumns
      .diff(intersectColumns ++ keys)
      .map(columnName => col(s"$aliasLeft.$columnName").as(columnName))
    val rightUniqColumns = rightColumns
      .diff(intersectColumns ++ keys)
      .map(columnName => col(s"$aliasRight.$columnName").as(columnName))
    val keysColumn = keys.map(columnName =>
      coalesce(col(s"$aliasLeft.$columnName"), col(s"$aliasRight.$columnName")).as(columnName)
    )

    joinedDf.select(keysColumn ++ coalesceColumns ++ leftUniqColumns ++ rightUniqColumns: _*)
  }

  def orderedDf(field: String)(df: DataFrame)(implicit sort: SortDirection): DataFrame =
    df
      .orderBy(orderColumn(field))

  def topN(groupField: Seq[Column], orderedField: String, top: Int)
          (df: DataFrame)(implicit sort: SortDirection): DataFrame = {
    val window = Window
      .partitionBy(groupField: _*)
      .orderBy(orderColumn(orderedField))
    df
      .withColumn("_rank", dense_rank().over(window))
      .filter(col("_rank") <= lit(top))
      .drop("_rank")
  }
}
