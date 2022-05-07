package org.veupathdb.lib.s3.s34k.minio

import io.minio.DeleteBucketTagsArgs
import io.minio.GetBucketTagsArgs
import io.minio.MinioClient
import io.minio.SetBucketTagsArgs
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.Bucket
import org.veupathdb.lib.s3.s34k.core.AbstractBucketTagContainer
import org.veupathdb.lib.s3.s34k.core.fields.BasicTagMap
import org.veupathdb.lib.s3.s34k.fields.TagMap
import org.veupathdb.lib.s3.s34k.minio.operations.BucketTagDeleter
import org.veupathdb.lib.s3.s34k.minio.util.*
import org.veupathdb.lib.s3.s34k.params.RegionRequestParams
import org.veupathdb.lib.s3.s34k.params.bucket.tag.TargetedBucketTagDeleteParams
import org.veupathdb.lib.s3.s34k.params.tag.*

internal class BucketTagContainer(
  private val bucket: Bucket,
  private val client: MinioClient
) : AbstractBucketTagContainer() {

  private val log = LoggerFactory.getLogger(this::class.java)

  override fun contains(key: String, params: TagExistsParams): Boolean {
    log.debug("Attempting to test whether {} contains tag '{}'", bucket, key)
    try {
      val out = key in getAllRaw(params)
      params.callback?.invoke(out)
      return out
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to test for tag '$key' on $bucket" }
    }
  }

  override fun count(params: TagCountParams): Int {
    log.debug("Attempting to count tags on {}", bucket)
    try {
      val out = getAllRaw(params).size
      params.callback?.invoke(out)
      return out
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to get tag count for $bucket" }
    }
  }

  override fun delete(params: TargetedBucketTagDeleteParams) {
    log.debug("Attempting to delete target tags from {}", bucket)
    BucketTagDeleter(bucket, client, params).execute()
  }

  override fun deleteAll(params: DeleteAllTagsParams) {
    log.debug("Attempting to delete all tags from {}", bucket)
    try {
      client.deleteBucketTags(DeleteBucketTagsArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())
      params.callback?.invoke()
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to delete all tags from $bucket" }
    }
  }

  override fun get(key: String): String? {
    log.debug("Attempting to get key '{}' from {}", key, bucket)
    try {
      val res = client.getBucketTags(GetBucketTagsArgs.builder()
        .bucket(bucket)
        .region(bucket)
        .build()).get()

      return res[key]
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to look up tag '$key' from $bucket" }
    }
  }

  override fun get(params: TagGetParams): TagMap {
    log.debug("Attempting to fetch target tags from {}", bucket)
    try {
      val res = getAllRaw(params)
      val tmp = HashMap<String, String>(params.tags.size)

      params.tags.forEach {
        if (it in res)
          tmp[it] = res[it]!!
      }

      val out = BasicTagMap(tmp)
      params.callback?.invoke(out)
      return out
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to fetch target tags from $bucket" }
    }
  }

  override fun getAll(): TagMap {
    log.debug("Attempting to fetch all tags from bucket {}", bucket)
    try {
      return BasicTagMap(client.getBucketTags(GetBucketTagsArgs.builder()
        .bucket(bucket)
        .region(bucket)
        .build()).get())
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to fetch all tags from $bucket" }
    }
  }

  override fun put(params: TagPutParams) {
    log.debug("Attempting to set {} tags on {}", params.tags, bucket)
    if (params.tags.isEmpty)
      return

    try {
      client.setBucketTags(SetBucketTagsArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        .tags(params.tags.toMap())
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())

      params.callback?.invoke()
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to set tags on $bucket" }
    }
  }

  @Suppress("NOTHING_TO_INLINE")
  private inline fun getAllRaw(params: RegionRequestParams): Map<String, String> {
    return client.getBucketTags(GetBucketTagsArgs.builder()
      .bucket(bucket)
      .region(params, bucket)
      .headers(params.headers)
      .queryParams(params.queryParams)
      .build()).get()
  }
}