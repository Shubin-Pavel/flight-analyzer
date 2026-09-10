package com.example
package config

import errors.ValidationErrorMessages
import types._

final case class JobConfig(
  airlinesPath: String,
  airportsPath: String,
  flightsPath: String,
  outputPath: String,
  sortDirection: SortDirection
)

object JobConfig {
  def fromArgs(args: Array[String]): JobConfig = {
    val provided: Int = args.length

    if (provided < 4 || provided > 5) {
      throw new IllegalArgumentException(ValidationErrorMessages.invalidArgumentsCount(provided))
    }

    JobConfig(
      airlinesPath = args(0),
      airportsPath = args(1),
      flightsPath = args(2),
      outputPath = args(3),
      sortDirection = if (provided == 5) SortDirection.fromString(args(4)) else Desc
    )
  }
}
