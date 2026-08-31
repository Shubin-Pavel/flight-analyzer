package com.example
package schemas

import org.apache.spark.sql.types._

object Schemas {
  val airlines: StructType = StructType(Seq(
    StructField("iata_code", StringType),
    StructField("airline_name", StringType)
  ))

  val airports: StructType = StructType(Seq(
    StructField("iata_code", StringType),
    StructField("airport", StringType),
    StructField("city", StringType),
    StructField("state", StringType),
    StructField("country", StringType),
    StructField("latitude", DecimalType(7,5)),
    StructField("longitude", DecimalType(8,5)),
  ))

  val flights: StructType = StructType(Seq(
    StructField("year", IntegerType),
    StructField("month", IntegerType),
    StructField("day", IntegerType),
    StructField("day_of_week", IntegerType),
    StructField("airline", StringType),
    StructField("flight_number", IntegerType),
    StructField("tail_number", StringType),
    StructField("origin_airport", StringType),
    StructField("destination_airport", StringType),
    StructField("scheduled_departure", IntegerType),
    StructField("departure_time", IntegerType),
    StructField("departure_delay", IntegerType),
    StructField("taxi_out", IntegerType),
    StructField("wheels_off", IntegerType),
    StructField("scheduled_time", IntegerType),
    StructField("elapsed_time", IntegerType),
    StructField("air_time", IntegerType),
    StructField("distance", IntegerType),
    StructField("wheels_on", IntegerType),
    StructField("taxi_in", IntegerType),
    StructField("scheduled_arrival", IntegerType),
    StructField("arrival_time", IntegerType),
    StructField("arrival_delay", IntegerType),
    StructField("diverted", IntegerType),
    StructField("cancelled", IntegerType),
    StructField("cancellation_reason", StringType),
    StructField("air_system_delay", IntegerType),
    StructField("security_delay", IntegerType),
    StructField("airline_delay", IntegerType),
    StructField("late_aircraft_delay", IntegerType),
    StructField("weather_delay", IntegerType),
  ))

  val originAirportAirline: StructType = StructType(Seq(
    StructField("origin_airport", StringType),
    StructField("airline_name", StringType),
    StructField("day_of_week", StringType),
    StructField("count", LongType),
    StructField("sum_dot", LongType),
    StructField("sum_aot", LongType),
    StructField("sum_cot", LongType),
  ))

  val originToDestination: StructType = StructType(Seq(
    StructField("origin_airport", StringType),
    StructField("destination_airport", StringType),
    StructField("count", LongType),
    StructField("sum_dot", LongType),
  ))

  val delay: StructType = StructType(Seq(
    StructField("count_air_system_delay", LongType),
    StructField("count_security_delay", LongType),
    StructField("count_airline_delay", LongType),
    StructField("count_late_aircraft_delay", LongType),
    StructField("count_weather_delay", LongType),
    StructField("sum_air_system_delay", LongType),
    StructField("sum_security_delay", LongType),
    StructField("sum_airline_delay", LongType),
    StructField("sum_late_aircraft_delay", LongType),
    StructField("sum_weather_delay", LongType),
    StructField("sum_departure_delay", LongType),
  ))

  val metaInfo: StructType = StructType(Seq(
    StructField("collected", DateType),
    StructField("processed", TimestampType),
    StructField("sorted", StringType),
  ))
}