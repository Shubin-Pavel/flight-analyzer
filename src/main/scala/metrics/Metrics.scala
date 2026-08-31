package com.example
package metrics

import transformers.Transformers._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Column}

object Metrics {

  def topAirports(df: DataFrame): DataFrame = {
    val groupColumns: Seq[Column] = Seq(col("origin_airport"))
    val aggColumns: Seq[Column] = Seq(sum(col("count")).as("total_flights"))
    df
      .transform(aggByGroup(groupColumns, aggColumns))
  }

  def topAirlineOnTime(df: DataFrame): DataFrame = {
    val groupColumns: Seq[Column] = Seq(col("airline_name"))
    val aggColumns: Seq[Column] = Seq(sum(col("sum_cot")).as("total_completed_on_time_flights"))
    df
      .transform(aggByGroup(groupColumns, aggColumns))
  }

  def carrierByAirport(df: DataFrame): DataFrame = {
    val groupColumns: Seq[Column] = Seq(col("origin_airport"), col("airline_name"))
    val aggColumns: Seq[Column] = Seq(sum(col("sum_dot")).as("total_departure_on_time_flights"))
    df
      .transform(aggByGroup(groupColumns, aggColumns))
  }

  def destinationByAirport(df: DataFrame): DataFrame = {
    val columnsList: Seq[Column] = Seq(
      col("origin_airport"),
      col("destination_airport"),
      col("sum_dot").as("total_departure_on_time_flights")
    )
    df
      .transform(selectColumns(columnsList))
  }

  def weekdayArrival(df: DataFrame): DataFrame = {
    val groupColumns: Seq[Column] = Seq(col("day_of_week"))
    val aggColumns: Seq[Column] = Seq(sum(col("sum_aot")).as("total_arrival_on_time_flights"))
    df
      .transform(aggByGroup(groupColumns, aggColumns))
  }

  def delayReasons(df: DataFrame): DataFrame = {
    val allDelay: Column = coalesce(col("sum_air_system_delay"), lit(0)) + coalesce(col("sum_security_delay"), lit(0)) +
      coalesce(col("sum_airline_delay"), lit(0)) + coalesce(col("sum_late_aircraft_delay"), lit(0)) +
      coalesce(col("sum_weather_delay"), lit(0))
    val columnsList: Seq[Column] = Seq(
      col("count_air_system_delay"),
      col("count_security_delay"),
      col("count_airline_delay"),
      col("count_late_aircraft_delay"),
      col("count_weather_delay"),
      (col("sum_air_system_delay")*lit(100.0)/allDelay).as("percent_air_system_delay"),
      (col("sum_security_delay")*lit(100.0)/allDelay).as("percent_security_delay"),
      (col("sum_airline_delay")*lit(100.0)/allDelay).as("percent_airline_delay"),
      (col("sum_late_aircraft_delay")*lit(100.0)/allDelay).as("percent_late_aircraft_delay"),
      (col("sum_weather_delay")*lit(100.0)/allDelay).as("percent_weather_delay"),
    )
    df
      .transform(selectColumns(columnsList))
  }
}
