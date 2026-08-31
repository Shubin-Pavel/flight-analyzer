package com.example
package config

import types._

case class JobConfig(
  airlinesPath: String,
  airportsPath: String,
  flightsPath: String,
  outputPath: String,
  sortDirection: SortDirection
)

object JobConfig {
  def fromArgs(args: Array[String]): JobConfig = {
    if (args.length < 4 || args.length > 5) {
      throw new IllegalArgumentException(
        "Usage: FlightAnalyzer <airlines.csv> <airports.csv> <flights.csv> <output> [asc|desc]"
      )
    }
    JobConfig(
      airlinesPath = args(0),
      airportsPath = args(1),
      flightsPath = args(2),
      outputPath = args(3),
      sortDirection = if (args.length == 5) SortDirection.fromString(args(4)) else Desc
    )
  }
}
