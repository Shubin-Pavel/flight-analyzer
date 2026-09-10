package com.example
package preprocessing

import validation.DfValidator
import transformers.Transformers._

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object RawData extends DfValidator {
  private val requiredAirlines: Seq[String] = Seq("iata_code", "airline_name")
  private val requiredAirports: Seq[String] = Seq("iata_code", "airport")
  private val requiredFlights: Seq[String] = Seq(
    "year", "month", "day", "day_of_week", "airline",
    "origin_airport", "destination_airport", "cancelled",
    "departure_delay", "arrival_delay", "air_system_delay",
    "security_delay", "airline_delay", "late_aircraft_delay", "weather_delay"
  )

  private def prepareReferenceData(
                                    df: DataFrame,
                                    requiredColumns: Seq[String],
                                    condition: Column,
                                    selectedColumns: Seq[Column],
                                    dfName: String
                                  ): DataFrame = {

    validateColumnPresence(requiredColumns)(df)

    val resultDf = df
      .transform(filterByCond(condition))
      .transform(selectColumns(selectedColumns))

    validateNotEmpty(resultDf, dfName)

    resultDf
  }

  def prepareAirlines(airlinesDf: DataFrame): DataFrame = prepareReferenceData(
    airlinesDf,
    requiredAirlines,
    col("iata_code").isNotNull,
    requiredAirlines.map(col),
    "airlines"
  )

  def prepareAirports(airportsDf: DataFrame): DataFrame = prepareReferenceData(
    airportsDf,
    requiredAirports,
    col("airport").isNotNull,
    Seq(col("airport"), col("iata_code")),
    "airports"
  )

  def prepareFlights(airlinesDf: DataFrame, airportsDf: DataFrame)(flightsDf: DataFrame): DataFrame = {
    validateColumnPresence(requiredFlights)(flightsDf)

    val isNullYear = col("year").isNull
    val isMonthValid = col("month").between(1, 12)
    val isDayValid = col("day").between(1, 31)
    val isDayOfWeekValid = col("day_of_week").between(1, 7)
    val isCancelled = col("cancelled") === lit(0)

    val resultDf = flightsDf.as("fl")
      .transform(filterByCond(!isNullYear && isMonthValid && isDayValid && isDayOfWeekValid && isCancelled))
      .transform(joinByCond(airlinesDf.as("al"), col("fl.airline") === col("al.iata_code"), "inner"))
      .transform(joinByCond(airportsDf.as("origin"), col("origin.iata_code") === col("fl.origin_airport"), "semi"))
      .transform(joinByCond(airportsDf.as("dest"), col("dest.iata_code") === col("fl.destination_airport"), "semi"))
      .transform(selectColumns(requiredFlights.map(col) ++ Seq(col("airline_name"))))
      .transform(withDelayInfo)
      .transform(withPeriodDt)

    validateNotEmpty(resultDf, "flights")

    resultDf
  }
}