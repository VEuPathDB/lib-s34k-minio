@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import io.minio.errors.ErrorResponseException
import io.minio.errors.MinioException
import org.veupathdb.lib.s3.s34k.S3ErrorCode
import org.veupathdb.lib.s3.s34k.errors.*
import org.veupathdb.lib.s3.s34k.fields.BucketName
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.contract


internal inline fun Throwable.throwCorrect(msg: () -> String) {
  if (this is S34kException)
    throw this
  if (this !is MinioException)
    throw this
  if (this !is ErrorResponseException)
    throw S34kException(msg(), this)

  val res = errorResponse()

  when (res.code()) {
    S3ErrorCode.NoSuchBucket            -> throw BucketNotFoundException(BucketName(res.bucketName()), this)
    S3ErrorCode.BucketAlreadyExists     -> throw BucketAlreadyExistsException(BucketName(res.bucketName()), this)
    S3ErrorCode.BucketAlreadyOwnedByYou -> throw BucketAlreadyOwnedByYouException(BucketName(res.bucketName()), this)
    S3ErrorCode.BucketNotEmpty          -> throw BucketNotEmptyException(BucketName(res.bucketName()), this)
    S3ErrorCode.NoSuchKey               -> throw ObjectNotFoundException(BucketName(res.bucketName()), res.objectName(), this)
  }

  throw S34kException(msg(), this)
}

@OptIn(ExperimentalContracts::class)
internal inline fun Throwable.isErrorResponse(): Boolean {
  contract {
    returns(true) implies (this@isErrorResponse is ErrorResponseException)
  }

  return this is ErrorResponseException
}

/**
 * Tests whether this exception is a `NoSuchBucket` error.
 */
internal inline fun Throwable.isNoSuchBucket() =
  isErrorResponse() && errorResponse().code() == S3ErrorCode.NoSuchBucket

/**
 * Tests whether this exception is a `BucketAlreadyOwnedByYou` error.
 */
internal inline fun Throwable.isBucketOwnedByYou() =
  isErrorResponse() && errorResponse().code() == S3ErrorCode.BucketAlreadyOwnedByYou

/**
 * Tests whether this exception is a `BucketAlreadyExists` error.
 */
internal inline fun Throwable.isBucketAlreadyExists() =
  isErrorResponse() && errorResponse().code() == S3ErrorCode.BucketAlreadyExists

/**
 * Tests whether this exception is a `BucketAlreadyExists` or
 * `BucketAlreadyOwnedByYou` error.
 */
internal inline fun Throwable.isBucketCollision() =
  isErrorResponse() && when (errorResponse().code()) {
    S3ErrorCode.BucketAlreadyExists     -> true
    S3ErrorCode.BucketAlreadyOwnedByYou -> true
    else                                -> false
  }

internal inline fun Throwable.toCorrect(msg: () -> String): Throwable {
  if (this is S34kException)
    return this
  if (this !is MinioException)
    return this
  if (this !is ErrorResponseException)
    return S34kException(msg(), this)

  val res = errorResponse()

  return when (res.code()) {
    S3ErrorCode.NoSuchBucket            -> BucketNotFoundException(BucketName(res.bucketName()), this)
    S3ErrorCode.BucketAlreadyExists     -> BucketAlreadyExistsException(BucketName(res.bucketName()), this)
    S3ErrorCode.BucketAlreadyOwnedByYou -> BucketAlreadyOwnedByYouException(BucketName(res.bucketName()), this)
    S3ErrorCode.BucketNotEmpty          -> BucketNotEmptyException(BucketName(res.bucketName()), this)
    S3ErrorCode.NoSuchKey               -> ObjectNotFoundException(BucketName(res.bucketName()), res.objectName(), this)
    else                                -> S34kException(msg(), this)
  }
}
