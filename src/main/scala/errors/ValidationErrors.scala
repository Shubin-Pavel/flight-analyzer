package com.example
package errors

object ValidationErrors {
  final case class MissingColumnsException(message: String)
    extends RuntimeException(message)

  final case class EmptyDataException(message: String)
    extends RuntimeException(message)
}
