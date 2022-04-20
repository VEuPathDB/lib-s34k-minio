package org.veupathdb.lib.s3.s34k.minio

import io.minio.errors.ErrorResponseException
import io.minio.errors.MinioException
import org.veupathdb.lib.s3.s34k.S3ErrorCode
import org.veupathdb.lib.s3.s34k.errors.*
import org.veupathdb.lib.s3.s34k.params.bucket.BucketName


internal inline fun Throwable.throwCorrect(bucket: BucketName, path: String, msg: () -> String) {
  if (this !is MinioException)
    throw this
  if (this !is ErrorResponseException)
    throw S34kException(msg(), this)

  when (errorResponse().code()) {
    S3ErrorCode.NoSuchBucket            -> throw BucketNotFoundException(bucket, this)
    S3ErrorCode.NoSuchKey               -> throw ObjectNotFoundException(bucket, path, this)
    S3ErrorCode.BucketAlreadyExists     -> throw BucketAlreadyExistsException(bucket, this)
    S3ErrorCode.BucketAlreadyOwnedByYou -> throw BucketAlreadyOwnedByYouException(bucket, this)
    S3ErrorCode.BucketNotEmpty          -> throw BucketNotEmptyException(bucket, this)
  }

  throw S34kException(msg(), this)
}

internal inline fun Throwable.throwCorrect(bucket: BucketName, msg: () -> String) {
  if (this !is MinioException)
    throw this
  if (this !is ErrorResponseException)
    throw S34kException(msg(), this)

  when (errorResponse().code()) {
    S3ErrorCode.NoSuchBucket            -> throw BucketNotFoundException(bucket, this)
    S3ErrorCode.BucketAlreadyExists     -> throw BucketAlreadyExistsException(bucket, this)
    S3ErrorCode.BucketAlreadyOwnedByYou -> throw BucketAlreadyOwnedByYouException(bucket, this)
    S3ErrorCode.BucketNotEmpty          -> throw BucketNotEmptyException(bucket, this)
  }

  throw S34kException(msg(), this)
}

internal inline fun MinioException.throwCorrect(bucket: BucketName, path: String, msg: () -> String) {
  if (this is ErrorResponseException) {
    when (errorResponse().code()) {
      S3ErrorCode.NoSuchBucket -> throw BucketNotFoundException(bucket, this)
      S3ErrorCode.NoSuchKey    -> throw ObjectNotFoundException(bucket, path, this)
    }
  }

  throw S34kException(msg(), this)
}

internal inline fun MinioException.throwCorrect(
  bucket: BucketName,
  msg: () -> String
) {
  if (this !is ErrorResponseException)
    throw S34kException(msg(), this)

  when (errorResponse().code()) {
    S3ErrorCode.BucketAlreadyOwnedByYou -> throw BucketAlreadyOwnedByYouException(bucket, this)
    S3ErrorCode.BucketAlreadyExists     -> throw BucketAlreadyExistsException(bucket, this)
    S3ErrorCode.NoSuchBucket            -> throw BucketNotFoundException(bucket, this)
  }

  throw S34kException(msg(), this)
}