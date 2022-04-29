@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import org.veupathdb.lib.s3.s34k.errors.InvalidRequestConfigException
import org.veupathdb.lib.s3.s34k.fields.BucketName
import org.veupathdb.lib.s3.s34k.requests.S3RequestParams
import org.veupathdb.lib.s3.s34k.requests.bucket.S3BucketRequestParams
import org.veupathdb.lib.s3.s34k.requests.`object`.S3ObjectRequestParams
import java.io.File


internal inline fun S3BucketRequestParams.reqBucket(): String {
  if (this.bucketName == null)
    throw InvalidRequestConfigException("Required field 'bucket' is not set", this)

  return this.bucketName!!.name
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
internal inline fun S3RequestParams.reqNonBlank(key: String, value: String) =
  value.ifBlank { throw InvalidRequestConfigException("Required field '$key' is not set.", this) }


internal inline fun <R> S3RequestParams.reqSet(name: String, value: R?) =
  value
    ?: throw InvalidRequestConfigException("Required field '$name' is not set.", this)

/**
 * Requires that the 'path' property on the receiver object is non-null and
 * non-blank.
 */
internal inline fun S3ObjectRequestParams.reqPath() =
  reqNonBlank("path", reqSet("path", path))

/**
 * Require Local File Exists
 */
internal inline fun File?.reqLFExists(params: S3RequestParams) =
  if (this == null)
    throw InvalidRequestConfigException("Required field 'localFile' is not set.", params)
  else if (!exists())
    throw InvalidRequestConfigException("Local file $this does not exist", params)
  else
    this