package org.veupathdb.lib.s3.s34k.minio.operations

import io.minio.DeleteBucketTagsArgs
import io.minio.GetBucketTagsArgs
import io.minio.MinioClient
import io.minio.SetBucketTagsArgs
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.core.fields.tags.BasicS3TagMap
import org.veupathdb.lib.s3.s34k.fields.tags.S3TagMap
import org.veupathdb.lib.s3.s34k.minio.*
import org.veupathdb.lib.s3.s34k.minio.headers
import org.veupathdb.lib.s3.s34k.minio.queryParams
import org.veupathdb.lib.s3.s34k.minio.regions
import org.veupathdb.lib.s3.s34k.minio.toCorrect
import org.veupathdb.lib.s3.s34k.requests.bucket.S3BucketTagDeleteParams
import org.veupathdb.lib.s3.s34k.response.bucket.S3Bucket

// TODO: v0.2.0
internal open class BucketTagDeleter(
  private val bucket: S3Bucket,
  private val minio:  MinioClient,
  private val params: S3BucketTagDeleteParams,
) {

  private val Log = LoggerFactory.getLogger(this::class.java)

  fun execute(): S3TagMap {
    // If allTags wasn't set, and there are no tags specified for deletion, then
    // we have nothing to do, so we can bail here.
    if (!params.allTags && params.tags.isEmpty)
      return params.callback.invoke("execute", Log, BasicS3TagMap())

    // Retrieve the tags currently attached to the target bucket.
    val oldTags = fetchTags()

    // If there aren't any tags presently attached to the bucket, then there is
    // nothing to delete; we can bail here.
    if (oldTags.isEmpty())
      return params.callback.invoke("execute", Log, BasicS3TagMap())

    val toDelete = HashMap<String, String>(params.tags.size)
    val toKeep   = HashMap<String, String>(oldTags.size)

    oldTags.forEach { (k, v) ->
      if (params.tags.contains(k))
        toDelete[k] = v
      else
        toKeep[k] = v
    }

    // If there is no overlap between the tags targeted for deletion, and the
    // tags presently on the bucket, then we have nothing we can delete; bail
    // here.
    if (toDelete.isEmpty())
      return params.callback.invoke("execute", Log, BasicS3TagMap())

    // If we are here, there exist tags on the target bucket that also exist in
    // the tags targeted for deletion.  We do not yet know if we will be
    // re-appending anything to the target bucket, but we will figure that out
    // later.

    deleteTags()

    // If we have nothing to re-append to the object, we can stop here and
    // return the map of tags we deleted.
    if (toKeep.isEmpty())
      return params.callback.invoke("execute", Log, BasicS3TagMap(toDelete))

    // Re-append the tags that we want to keep to the target bucket.
    appendTags(toKeep)

    return params.callback.invoke("execute", Log, BasicS3TagMap(toDelete))
  }

  protected fun appendTags(tags: Map<String, String>) {
    Log.trace("appendTags()")

    try {
      Log.debug("Re-appending {} tags to bucket '{}'", tags.size, bucket.bucketName)

      minio.setBucketTags(SetBucketTagsArgs.builder()
        .bucket(bucket.bucketName.name)
        .regions(params.region, bucket.defaultRegion, bucket.client.defaultRegion)
        .tags(tags)
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())

      Log.debug("Successfully re-appended {} tags on bucket '{}'", tags.size, bucket.bucketName)
    } catch (e: Throwable) {
      Log.error("Failed to re-append tags to bucket '{}'", bucket.bucketName)
      throw e.toCorrect { "Failed to re-append tags to bucket '${bucket.bucketName}'" }
    }

  }

  protected fun deleteTags() {
    Log.trace("deleteTags()")

    try {
      Log.debug("Deleting all tags from bucket '{}'.", bucket.bucketName)

      minio.deleteBucketTags(DeleteBucketTagsArgs.builder()
        .bucket(bucket.bucketName.name)
        .regions(params.region, bucket.defaultRegion, bucket.client.defaultRegion)
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())

      Log.debug("Successfully deleted all tags from bucket '{}'.", bucket.bucketName)
    } catch (e: Throwable) {
      Log.error("Failed to delete tags from bucket '{}'", bucket.bucketName)
      throw e.toCorrect { "Failed to delete tags from bucket '${bucket.bucketName}'" }
    }
  }

  protected fun fetchTags(): Map<String, String> {
    Log.trace("fetchTags()")

    try {
      Log.debug("Fetching all tags for bucket '{}'.", bucket.bucketName)

      val res = minio.getBucketTags(GetBucketTagsArgs.builder()
        .bucket(bucket.bucketName.name)
        .regions(params.region, bucket.defaultRegion, bucket.client.defaultRegion)
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build()).get()

      Log.debug("Successfully fetched {} tags from bucket '{}'.", res.size, bucket.bucketName)

      return res
    } catch (e: Throwable) {
      Log.error("Failed to fetch tags from bucket '{}'", bucket.bucketName)
      throw e.toCorrect { "Failed to fetch tags from bucket '${bucket.bucketName}'" }
    }
  }
}