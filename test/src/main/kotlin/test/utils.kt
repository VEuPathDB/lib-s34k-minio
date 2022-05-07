@file:Suppress("NOTHING_TO_INLINE")

package test

import org.slf4j.Logger
import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.buckets.S3Bucket
import org.veupathdb.lib.s3.s34k.fields.BucketName
import java.util.*

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

private const val bucketChars = "abcdefghijklmnopqrstuvwxyz0123456789"


internal inline fun S3Client.withBucket(Log: Logger, action: S3Bucket.() -> Boolean): Boolean {
  val name = BucketName(randomName())

  Log.debug("Setup: creating test bucket '{}'", name)

  try {
    return buckets.create(name).action()
  } finally {
    if (buckets.exists(name)) {
      Log.debug("Cleanup: removing bucket '{}'", name)
      buckets.deleteRecursive(name)
    }
  }
}

private fun randomName(): String {
  val out = StringBuilder(63)
  val ran = Random(System.currentTimeMillis())

  for (i in 0..62) {
    out.append(bucketChars[ran.nextInt(36)])
  }

  return out.toString()
}
