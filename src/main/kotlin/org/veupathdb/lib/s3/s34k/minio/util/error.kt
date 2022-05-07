@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio.util

import io.minio.errors.ErrorResponseException
import io.minio.errors.MinioException
import org.veupathdb.lib.s3.s34k.BucketName
import org.veupathdb.lib.s3.s34k.S3ErrorCode
import org.veupathdb.lib.s3.s34k.errors.*

internal inline fun Throwable.isNoSuchKey() =
  this is ErrorResponseException && isNoSuchKey()

internal inline fun Throwable.isNoSuchBucket() =
  this is ErrorResponseException && isNoSuchBucket()

internal inline fun Throwable.isBucketConflict() =
  this is ErrorResponseException && (isBucketAlreadyExists() || isBucketAlreadyOwnedByYou())

internal inline fun Throwable.isBucketAlreadyExists() =
  this is ErrorResponseException && isBucketAlreadyExists()

internal inline fun Throwable.isBucketAlreadyOwnedByYou() =
  this is ErrorResponseException && isBucketAlreadyOwnedByYou()


//


internal inline fun ErrorResponseException.isNoSuchKey() =
  errorResponse().code() == S3ErrorCode.NoSuchKey

internal inline fun ErrorResponseException.isBucketAlreadyExists() =
  errorResponse().code() == S3ErrorCode.BucketAlreadyExists

internal inline fun ErrorResponseException.isBucketAlreadyOwnedByYou() =
  errorResponse().code() == S3ErrorCode.BucketAlreadyOwnedByYou

internal inline fun ErrorResponseException.isNoSuchBucket() =
  errorResponse().code() == S3ErrorCode.NoSuchBucket


//


internal inline fun Throwable.throwCorrect(msg: () -> String): Nothing {
  if (this is S34KError)
    throw this
  if (this !is MinioException)
    throw this
  if (this !is ErrorResponseException)
    throw S34KError(msg(), this)

  val res = errorResponse()

  throw when (res.code()) {
    S3ErrorCode.NoSuchBucket            -> BucketNotFoundError(BucketName(res.bucketName()), this)
    S3ErrorCode.NoSuchKey               -> ObjectNotFoundError(BucketName(res.bucketName()), res.objectName(), this)
    S3ErrorCode.BucketNotEmpty          -> BucketNotEmptyError(BucketName(res.bucketName()), this)
    S3ErrorCode.BucketAlreadyExists     -> BucketAlreadyExistsError(BucketName(res.bucketName()), this)
    S3ErrorCode.BucketAlreadyOwnedByYou -> BucketAlreadyOwnedByYouError(BucketName(res.bucketName()), this)
    else                                -> S34KError(msg(), this)
  }
}
