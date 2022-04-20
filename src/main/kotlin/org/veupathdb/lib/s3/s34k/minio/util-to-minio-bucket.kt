@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import io.minio.*
import org.veupathdb.lib.s3.s34k.params.DeleteParams
import org.veupathdb.lib.s3.s34k.params.bucket.*

// region Tag Params

internal inline fun BucketTagDeleteParams.toBucketTagDelete(bucket: BucketName, region: String?) =
  DeleteBucketTagsArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun BucketTagDeleteParams.toBucketTagFetch(bucket: BucketName, region: String?) =
  GetBucketTagsArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun BucketTagDeleteParams.toBucketTagInsert(
  bucket: BucketName,
  region: String?,
  tags:   Map<String, String>,
) =
  SetBucketTagsArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.tags(tags)
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun BucketTagGetParams.toMinio(name: BucketName, region: String?) =
  GetBucketTagsArgs.builder().also {
    it.bucket(name.name)
    it.region(this.region ?: region)
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()


internal inline fun BucketTagPutParams.toMinio(name: BucketName, region: String?) =
  SetBucketTagsArgs.builder().also {
    it.bucket(name.name)
    it.region(this.region ?: region)
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
    it.tags(getTagsMap())
  }.build()

// endregion Tag Params

internal inline fun BucketExistsParams.toMinio(region: String?) =
  BucketExistsArgs.builder().also {
    it.bucket(reqBucket().name)
    it.region(this.region ?: region)
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()


internal inline fun BucketListParams.toMinio() =
  ListBucketsArgs.builder().also {
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()


internal inline fun BucketGetParams.toMinio() =
  ListBucketsArgs.builder().also {
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()


internal inline fun BucketPutParams.toMinio(region: String?) =
  MakeBucketArgs.builder().also {
    it.bucket(reqBucket().name)
    it.region(this.region ?: region)
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()


internal inline fun BucketDeleteParams.toMinio(region: String?) =
  RemoveBucketArgs.builder().also {
    it.bucket(reqBucket().name)
    it.region(this.region ?: region)
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun DeleteParams.toMinio(bucket: BucketName, region: String?) =
  RemoveBucketArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }
