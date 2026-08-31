package com.example
package preprocessing

import transformers.Transformers._

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._

object DataMart {
  def originAirportAirlineDataMart(df: DataFrame): DataFrame = {
    val groupColumns: Seq[Column] = Seq(col("origin_airport"), col("airline_name"), col("day_of_week"))
    val aggColumns: Seq[Column] = Seq(
      sum(lit(1)).as("count"),
      sum(when(!col("is_departure_delay"), lit(1))).as("sum_dot"),
      sum(when(!col("is_arrival_delay"), lit(1))).as("sum_aot"),
      sum(when(!col("is_arrival_delay") && !col("is_departure_delay"), lit(1))).as("sum_cot"),
    )
    df
      .transform(aggByGroup(groupColumns, aggColumns))
  }

  def originToDestinationDataMart(df: DataFrame): DataFrame = {
    val groupColumns: Seq[Column] = Seq(col("origin_airport"), col("destination_airport"))
    val aggColumns: Seq[Column] = Seq(
      sum(lit(1)).as("count"),
      sum(when(!col("is_departure_delay"), lit(1))).as("sum_dot"),
    )
    df
      .transform(aggByGroup(groupColumns, aggColumns))
  }

  def delayDataMart(df: DataFrame): DataFrame = {
    val groupColumns: Seq[Column] = Seq.empty[Column]
    val aggColumns: Seq[Column] = Seq(
      sum(when(col("is_air_system_delay"), lit(1))).as("count_air_system_delay"),
      sum(when(col("is_security_delay"), lit(1))).as("count_security_delay"),
      sum(when(col("is_airline_delay"), lit(1))).as("count_airline_delay"),
      sum(when(col("is_late_aircraft_delay"), lit(1))).as("count_late_aircraft_delay"),
      sum(when(col("is_weather_delay"), lit(1))).as("count_weather_delay"),
      sum(when(col("is_air_system_delay"), col("air_system_delay"))).as("sum_air_system_delay"),
      sum(when(col("is_security_delay"),col("security_delay"))).as("sum_security_delay"),
      sum(when(col("is_airline_delay"), col("airline_delay"))).as("sum_airline_delay"),
      sum(when(col("is_late_aircraft_delay"), col("late_aircraft_delay"))).as("sum_late_aircraft_delay"),
      sum(when(col("is_weather_delay"), col("weather_delay"))).as("sum_weather_delay"),
      sum(when(col("is_departure_delay"), col("departure_delay"))).as("sum_departure_delay"),
      max(col("period_dt")).as("max_period_dt"),
    )
    df
      .transform(aggByGroup(groupColumns, aggColumns))
  }

}
