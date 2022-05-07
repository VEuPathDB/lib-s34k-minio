package org.veupathdb.lib.s3.s34k.minio.operations

import io.minio.*
import io.minio.messages.Item
import io.minio.messages.DeleteObject
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.BucketName
import org.veupathdb.lib.s3.s34k.minio.util.*
import org.veupathdb.lib.s3.s34k.params.bucket.recursive.RecursiveBucketDeleteError
import org.veupathdb.lib.s3.s34k.params.bucket.recursive.RecursiveBucketDeleteObjectDeleteError
import org.veupathdb.lib.s3.s34k.params.bucket.recursive.RecursiveBucketDeleteParams
import org.veupathdb.lib.s3.s34k.params.bucket.recursive.RecursiveDeletePhase
import org.veupathdb.lib.s3.s34k.params.`object`.ObjectDeleteError
import java.util.stream.Stream

internal class RecursiveBucketDeleter(
  private val bucket: BucketName,
  private val region: String?,
  private val params: RecursiveBucketDeleteParams,
  private val client: MinioClient
) {

  private val log = LoggerFactory.getLogger(this::class.java)

  fun execute() {
    deleteObjects(listObjects())
    deleteBucket()
  }

  private fun listObjects(): Stream<String> {
    log.debug("Attempting to list all objects in bucket '{}'", bucket)
    return try {
      client.listObjects(ListObjectsArgs.builder()
        .bucket(bucket.name)
        .region(params.region ?: region)
        .recursive(true)
        .maxKeys(1000) // TODO: This should be configurable
        .headers(params.headers, params.objectFetch.headers)
        .queryParams(params.queryParams, params.objectFetch.queryParams)
        .build())
        .toStream()
        .map(Result<Item>::get)
        .map(Item::objectName)
    } catch (e: Throwable) {
      throw RecursiveBucketDeleteError(bucket, RecursiveDeletePhase.ListObjects, params, e)
    }

  }

  private fun deleteObjects(stream: Stream<String>) {
    log.debug("Attempting to delete all objects in bucket '{}'", bucket)
    try {
      val res = client.removeObjects(RemoveObjectsArgs.builder()
        .bucket(bucket.name)
        .region(params.region ?: region)
        .objects(stream.map(::DeleteObject).toIterable())
        .headers(params.headers, params.objectDelete.headers)
        .queryParams(params.queryParams, params.objectDelete.queryParams)
        .build())

      val errs = ArrayList<ObjectDeleteError>(10)

      res.forEach {
        val t = it.get()
        errs.add(ObjectDeleteError(t.objectName(), t.message(), t.code()))
      }

      if (errs.isNotEmpty())
        throw RecursiveBucketDeleteObjectDeleteError(bucket, params, errs)
    } catch (e: Throwable) {
      if (e !is RecursiveBucketDeleteError)
        throw RecursiveBucketDeleteError(bucket, RecursiveDeletePhase.DeleteObjects, params, e)
      else
        throw e
    }
  }

  private fun deleteBucket() {
    try {
      client.removeBucket(RemoveBucketArgs.builder()
        .bucket(bucket.name)
        .region(params.region ?: region)
        .headers(params.headers, params.bucketDelete.headers)
        .queryParams(params.queryParams, params.bucketDelete.queryParams)
        .build())
    } catch (e: Throwable) {
      throw RecursiveBucketDeleteError(bucket, RecursiveDeletePhase.DeleteBucket, params, e)
    }
  }
}