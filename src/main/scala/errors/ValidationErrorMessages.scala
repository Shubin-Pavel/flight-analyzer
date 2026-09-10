package com.example
package errors

object ValidationErrorMessages {
  def invalidArgumentsCount(provided: Int): String =
    s"""|Error: Invalid number of arguments (provided $provided).
        |
        |Usage:
        | FlightAnalyzer <input-file1> <input-file2> <input-file3> <output-file> [asc|desc]
        |
        |Arguments:
        | <input-file1> Path to the airlines data file in CSV format
        | <input-file2> Path to the airports data file in CSV format
        | <input-file3> Path to the flights data file in CSV format
        | <output-file> Path where the analyzed data will be saved
        | [asc|desc] Optional sorting direction, defaults to desc
        |""".stripMargin

  def invalidSortDirection(direction: String): String =
    s"Sort direction must be asc or desc, provided [$direction]"

}
