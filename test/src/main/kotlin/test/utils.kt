@file:Suppress("NOTHING_TO_INLINE")

package test

import org.slf4j.Logger

internal inline fun Logger.succeed(): Boolean {
  debug("Success!")
  return true
}

internal inline fun Logger.fail(e: Throwable): Boolean {
  error("Failed with exception!", e)
  return false
}

internal inline fun Logger.fail(msg: String): Boolean {
  error("Failed: {}", msg)
  return false
}