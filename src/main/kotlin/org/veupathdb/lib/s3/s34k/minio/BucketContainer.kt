package org.veupathdb.lib.s3.s34k.minio

import io.minio.BucketExistsArgs
import io.minio.ListBucketsArgs
import io.minio.MinioClient

import org.slf4j.LoggerFactory

import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.buckets.BucketList
import org.veupathdb.lib.s3.s34k.buckets.S3Bucket
import org.veupathdb.lib.s3.s34k.core.buckets.AbstractBucketContainer
import org.veupathdb.lib.s3.s34k.core.buckets.BasicBucketList
import org.veupathdb.lib.s3.s34k.params.S3RequestParams
import org.veupathdb.lib.s3.s34k.params.bucket.*
import org.veupathdb.lib.s3.s34k.params.bucket.put.BucketPutParams
import org.veupathdb.lib.s3.s34k.params.bucket.put.BucketUpsertParams
import org.veupathdb.lib.s3.s34k.params.bucket.recursive.RecursiveBucketDeleteParams

import org.veupathdb.lib.s3.s34k.errors.BucketNotFoundError
import org.veupathdb.lib.s3.s34k.fields.BucketName
import org.veupathdb.lib.s3.s34k.minio.operations.BucketDelete

import org.veupathdb.lib.s3.s34k.minio.operations.BucketInsert
import org.veupathdb.lib.s3.s34k.minio.operations.BucketUpsert
import org.veupathdb.lib.s3.s34k.minio.operations.RecursiveBucketDeleter
import org.veupathdb.lib.s3.s34k.minio.util.*
import java.util.stream.Collectors


internal class BucketContainer(
  private val client: S3Client,
  private val minio: MinioClient,
) : AbstractBucketContainer() {

  private val log = LoggerFactory.getLogger(this::class.java)


  override fun create(name: BucketName, params: BucketPutParams): S3Bucket {
    log.debug("Attempting to create bucket '{}'", name)
    return BucketInsert.execute(name, params, client.defaultRegion, minio)
  }


  override fun createIfNotExists(name: BucketName, params: BucketUpsertParams): S3Bucket {
    log.debug("Attempting to create bucket '{}'", name)
    return BucketUpsert.execute(name, params, client.defaultRegion, minio)
  }

  override fun delete(name: BucketName, params: BucketDeleteParams) {
    log.debug("Attempting to delete bucket '{}'", name)
    BucketDelete(name, client.defaultRegion, params, minio)
  }

  override fun deleteRecursive(name: BucketName, params: RecursiveBucketDeleteParams) {
    log.debug("Attempting to recursively delete bucket '{}'", name)
    RecursiveBucketDeleter(name, client.defaultRegion, params, minio).execute()
  }

  override fun exists(name: BucketName, params: BucketExistsParams): Boolean {
    log.debug("Attempting to test whether bucket '{}' exists", name)
    try {
      val out = minio.bucketExists(BucketExistsArgs.builder()
        .bucket(name.name)
        .region(params.region ?: client.defaultRegion)
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())
      params.callback?.invoke(out)
      return out
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to test whether bucket '$name' exists" }
    }
  }

  override fun get(name: BucketName, params: BucketGetParams): S3Bucket? {
    log.debug("Attempting to get bucket '{}'", name)
    try {
      val out = getAllRaw(params).hunt(name, client.defaultRegion, minio)
      params.callback?.invoke(out)
      return out
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to get bucket '$name'" }
    }
  }

  override fun list(params: BucketListParams): BucketList {
    log.debug("Attempting to get bucket list")
    try {
      val out = BasicBucketList(getAllRaw(params)
        .stream()
        .map { MBucket(BucketName(it.name()), client.defaultRegion, it.creationDate().toOffsetDateTime(), minio) }
        .collect(Collectors.toList()))

      params.callback?.invoke(out)

      return out
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to get a list of all buckets" }
    }
  }

  override fun <R> withBucket(name: BucketName, action: S3Bucket.() -> R) =
    get(name)?.action() ?: throw BucketNotFoundError(name)

  @Suppress("NOTHING_TO_INLINE")
  private inline fun getAllRaw(params: S3RequestParams) =
    minio.listBuckets(ListBucketsArgs.builder()
      .headers(params.headers)
      .queryParams(params.queryParams)
      .build())
}