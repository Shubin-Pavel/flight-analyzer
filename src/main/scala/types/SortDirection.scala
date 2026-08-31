package com.example
package types

sealed trait SortDirection
case object Asc extends SortDirection
case object Desc extends SortDirection
object SortDirection {
  def fromString(value: String): SortDirection = value.toLowerCase match {
    case "asc"  => Asc
    case "desc" => Desc
    case other => throw new IllegalArgumentException(
      s"Sort direction must be asc or desc, got [$other]"
    )
  }
}