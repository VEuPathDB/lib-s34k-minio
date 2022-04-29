@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import io.minio.*
import org.veupathdb.lib.s3.s34k.fields.BucketName
import org.veupathdb.lib.s3.s34k.requests.bucket.S3BucketTagDeleteParams

// region Tag Params

internal inline fun S3BucketTagDeleteParams.toBucketTagDelete(bucket: BucketName, region: String?) =
  DeleteBucketTagsArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun S3BucketTagDeleteParams.toBucketTagFetch(bucket: BucketName, region: String?) =
  GetBucketTagsArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun S3BucketTagDeleteParams.toBucketTagInsert(
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


// endregion Tag Params


