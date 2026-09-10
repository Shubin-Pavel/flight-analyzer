package com.example
package types

import errors.ValidationErrorMessages

sealed trait SortDirection
case object Asc extends SortDirection
case object Desc extends SortDirection
object SortDirection {
  def fromString(value: String): SortDirection = value.toLowerCase match {
    case "asc"  => Asc
    case "desc" => Desc
    case other => throw new IllegalArgumentException(ValidationErrorMessages.invalidSortDirection(other))
  }
}