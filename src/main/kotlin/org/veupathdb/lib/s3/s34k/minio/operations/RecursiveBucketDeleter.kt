package org.veupathdb.lib.s3.s34k.minio.operations

import io.minio.ListObjectsArgs
import io.minio.MinioClient
import io.minio.RemoveBucketArgs
import io.minio.RemoveObjectsArgs
import io.minio.Result
import io.minio.messages.DeleteObject
import io.minio.messages.Item
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.minio.*
import org.veupathdb.lib.s3.s34k.minio.DummyIterable
import org.veupathdb.lib.s3.s34k.minio.isNoSuchBucket
import org.veupathdb.lib.s3.s34k.minio.reqSet
import org.veupathdb.lib.s3.s34k.minio.toMultiMap
import org.veupathdb.lib.s3.s34k.requests.bucket.recursive.RecursiveBucketDeleteError
import org.veupathdb.lib.s3.s34k.requests.bucket.recursive.RecursiveBucketDeleteObjectDeleteError
import org.veupathdb.lib.s3.s34k.requests.bucket.recursive.S3ClientRecursiveBucketDeleteParams
import org.veupathdb.lib.s3.s34k.requests.bucket.recursive.S3RecursiveDeletePhase
import org.veupathdb.lib.s3.s34k.requests.`object`.ObjectDeleteError
import java.util.stream.Stream

// TODO: apply global headers to all requests
internal open class RecursiveBucketDeleter(
  client: S3Client,
  private val minio:  MinioClient,
  private val params: S3ClientRecursiveBucketDeleteParams,
) {

  private val Log = LoggerFactory.getLogger(this::class.java)

  // TODO: v0.2.0 - This should be a cascading lookup for: params.region || bucket.region || client.region || null
  private val region = params.region ?: client.defaultRegion

  // TODO: v0.2.0 - This should be in the params
  private val pageSize = 100

  fun execute() {
    Log.trace("execute()")

    try {
      deleteObjects(minio, params, listObjects(minio, params))
      deleteBucket(minio, params)
    } catch (e: RecursiveBucketDeleteError) {
      throw e.also { it.cause?.also { if (it.isNoSuchBucket()) return } }
    }
  }

  protected fun listObjects(
    minio:  MinioClient,
    params: S3ClientRecursiveBucketDeleteParams
  ): Stream<String> {
    Log.trace("listObjects(minio = {}, params = {})", minio, params)

    try {
      Log.debug("Fetching list of objects in bucket '{}'", params.bucketName)

      val iterable = minio.listObjects(ListObjectsArgs.builder()
        .bucket(params.reqSet("bucket", params.bucketName).name)
        .region(region)
        .recursive(true)
        .maxKeys(pageSize)
        .extraHeaders(params.objectFetch.headers.toMultiMap(params.headers))
        .extraQueryParams(params.objectFetch.queryParams.toMultiMap(params.queryParams))
        .build())

      Log.debug("Successfully fetched list of objects in bucket '{}'", params.bucketName)

      return iterable.toStream()
        .map(Result<Item>::get)
        .filter { !it.isDeleteMarker }
        .map(Item::objectName)
    } catch (e: Throwable) {
      Log.error("Failed to fetch list of objects in bucket '{}'", params.bucketName)

      throw RecursiveBucketDeleteError(
        S3RecursiveDeletePhase.ListObjects,
        params,
        "Failed to fetch list of objects in bucket '${params.bucketName}'",
        e
      )
    }
  }

  protected fun deleteObjects(
    minio:  MinioClient,
    params: S3ClientRecursiveBucketDeleteParams,
    input:  Stream<String>
  ) {
    Log.trace("deleteObjects(minio = {}, params = {}, input = {})", minio, params, input)

    try {
      Log.debug("Removing all objects from bucket '{}'", params.bucketName)

      val res = minio.removeObjects(RemoveObjectsArgs.builder()
        .bucket(params.reqSet("bucket", params.bucketName).name)
        .region(region)
        .objects(DummyIterable(input.map { DeleteObject(it) }.iterator()))
        .extraHeaders(params.objectDelete.headers.toMultiMap(params.headers))
        .extraQueryParams(params.objectDelete.queryParams.toMultiMap(params.queryParams))
        // TODO: v0.2.0 - Governance mode
        .build())

      Log.debug("Bulk removal operation on bucket '{}' succeeded.", params.bucketName)

      Log.debug("Testing to confirm that all objects were deleted from bucket '{}'.", params.bucketName)

      val failed = ArrayList<ObjectDeleteError>(32)

      res.forEach { failed.add(with(it.get()) { ObjectDeleteError(objectName(), message(), code()) }) }

      if (failed.isEmpty()) {
        Log.debug("No object failures found.")
        return
      }

      Log.warn("Failed to delete '{}' objects from bucket '{}'.", failed.size, params.bucketName)

      throw RecursiveBucketDeleteObjectDeleteError(params, failed)
    } catch (e: Throwable) {
      if (!e.isNoSuchBucket())
        Log.error("Failed to delete all objects from bucket '{}'.", params.bucketName)

      if (e is RecursiveBucketDeleteObjectDeleteError)
        throw e

      throw RecursiveBucketDeleteError(
        S3RecursiveDeletePhase.DeleteObjects,
        params,
        "Failed to delete all objects from bucket '${params.bucketName}'.",
        e
      )
    }
  }

  protected fun deleteBucket(
    minio:  MinioClient,
    params: S3ClientRecursiveBucketDeleteParams,
  ) {
    Log.trace("deleteBucket(minio = {}, params = {})", minio, params)

    try {
      Log.debug("Attempting to delete bucket '{}'", params.bucketName)

      minio.removeBucket(RemoveBucketArgs.builder()
        .bucket(params.bucketName!!.name)
        .region(region)
        .extraHeaders(params.bucketDelete.headers.toMultiMap(params.headers))
        .extraQueryParams(params.bucketDelete.queryParams.toMultiMap(params.queryParams))
        .build())

      Log.debug("Successfully deleted bucket '{}'", params.bucketName)
    } catch (e: Throwable) {
      Log.error("Failed to delete bucket '{}'.", params.bucketName)

      throw RecursiveBucketDeleteError(
        S3RecursiveDeletePhase.DeleteBucket,
        params,
        "Failed to delete bucket '${params.bucketName}'",
        e
      )
    }
  }
}