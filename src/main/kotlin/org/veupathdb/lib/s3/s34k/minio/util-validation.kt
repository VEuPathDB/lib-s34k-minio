@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import org.veupathdb.lib.s3.s34k.errors.InvalidRequestConfigException
import org.veupathdb.lib.s3.s34k.params.BaseRequest
import org.veupathdb.lib.s3.s34k.params.bucket.BaseBucketRequest
import org.veupathdb.lib.s3.s34k.params.bucket.BucketName
import org.veupathdb.lib.s3.s34k.params.`object`.BaseObjectRequest
import java.io.File


internal inline fun BaseBucketRequest.reqBucket(): BucketName {
  if (this.bucket == null)
    throw InvalidRequestConfigException("Required field 'bucket' is not set", this)

  return this.bucket!!
}

internal inline fun BucketName?.require(p: BaseRequest): BucketName {
  if (this == null)
    throw InvalidRequestConfigException("Required field 'bucket' is not set.", p)

  return this
}

/**
 * Requires that the given [value] is set (non-blank).
 *
 * If the given [value] string is blank, an [InvalidRequestConfigException] will
 * be thrown.
 *
 * @param key Name of the required value, used when constructing the exception
 * message.
 *
 * @param value Value to test.
 */
internal inline fun BaseRequest.reqNonBlank(key: String, value: String) =
  value.ifBlank { throw InvalidRequestConfigException("Required field '$key' is not set.", this) }


internal inline fun <R> BaseRequest.reqSet(name: String, value: R?) =
  value
    ?: throw InvalidRequestConfigException("Required field '$name' is not set.", this)


/**
 * Requires that the 'path' property on the receiver object is non-null and
 * non-blank.
 */
internal inline fun BaseObjectRequest.reqPath() =
  reqNonBlank("path", reqSet("path", path))


/**
 * Executes the given function if the receiver value is not-null.
 *
 * @param fn Function to execute.
 */
internal inline fun <I, R> I?.ifNotNull(fn: (I) -> R) {
  if (this != null)
    fn(this)
}

/**
 * Require Local File Exists
 */
internal inline fun File?.reqLFExists(params: BaseRequest) =
  if (this == null)
    throw InvalidRequestConfigException("Required field 'localFile' is not set.", params)
  else if (!exists())
    throw InvalidRequestConfigException("Local file $this does not exist", params)
  else
    this