package org.veupathdb.lib.s3.s34k.minio

import io.minio.errors.ErrorResponseException
import io.minio.errors.MinioException
import org.veupathdb.lib.s3.s34k.S3ErrorCode
import org.veupathdb.lib.s3.s34k.errors.BucketNotFoundException
import org.veupathdb.lib.s3.s34k.errors.ObjectNotFoundException
import org.veupathdb.lib.s3.s34k.errors.S34kException

internal inline fun MinioException.throwCorrect(bucket: String, path: String, msg: () -> String) {
  if (this is ErrorResponseException) {
    when (errorResponse().code()) {
      S3ErrorCode.NoSuchBucket -> throw BucketNotFoundException(bucket, this)
      S3ErrorCode.NoSuchKey    -> throw ObjectNotFoundException(bucket, path, this)
    }
  }

  throw S34kException(msg(), this)
}

internal inline fun MinioException.throwCorrect(bucket: String, msg: () -> String) {
  if (this is ErrorResponseException && errorResponse().code() == S3ErrorCode.NoSuchBucket)
    throw BucketNotFoundException(bucket, this)

  throw S34kException(msg(), this)
}