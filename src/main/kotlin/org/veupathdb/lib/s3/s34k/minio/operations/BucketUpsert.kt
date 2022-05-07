package org.veupathdb.lib.s3.s34k.minio.operations

import io.minio.ListBucketsArgs
import io.minio.MakeBucketArgs
import io.minio.MinioClient
import io.minio.SetBucketTagsArgs
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.buckets.S3Bucket
import org.veupathdb.lib.s3.s34k.errors.BucketNotFoundError
import org.veupathdb.lib.s3.s34k.fields.BucketName
import org.veupathdb.lib.s3.s34k.minio.util.*
import org.veupathdb.lib.s3.s34k.params.bucket.put.BucketUpsertParams

internal object BucketUpsert {

  private val log = LoggerFactory.getLogger(this::class.java)

  fun execute(bucket: BucketName, params: BucketUpsertParams, region: String?, minio: MinioClient): S3Bucket {
    // If we just created the bucket _or_ if the put tags on collision parameter
    // is set.
    //
    // If we just created the bucket, then we want to append the tags.
    // If we did not just create the bucket, but the putTagsIfCollision is set
    //    then we want to append the tags.
    if (putBucket(bucket, params, region, minio) || params.putTagsIfCollision)
      putBucketTags(bucket, params, region, minio)

    // S3 race conditions, yo.  Some doofus probably deleted the bucket right
    // between that last step and here.
    val out = getBucket(bucket, params, region, minio)
      ?: throw BucketNotFoundError(bucket, "Bucket lookup after creation failed!")

    // Allert the press, the process completed successfully.
    params.callback?.invoke(out)

    return out
  }

  private fun putBucket(bucket: BucketName, params: BucketUpsertParams, region: String?, minio: MinioClient): Boolean {
    log.debug("Attempting to create bucket '{}'", bucket)
    try {
      minio.makeBucket(MakeBucketArgs.builder()
        .bucket(bucket.name)
        .region(params.region ?: region)
        .headers(params.headers, params.putParams.headers)
        .queryParams(params.queryParams, params.putParams.queryParams)
        .build())

      params.putParams.callback?.invoke()
    } catch (e: Throwable) {
      // If the bucket already existed, that's fine.
      if (e.isBucketConflict())
        return false

      e.throwCorrect { "Failed to put bucket '$bucket'" }
    }

    return true
  }

  private fun putBucketTags(bucket: BucketName, params: BucketUpsertParams, region: String?, minio: MinioClient) {
    log.debug("Attaching tags to bucket '{}'", bucket)

    if (params.tags.isEmpty) {
      params.tagPutParams.callback?.invoke()
      return
    }

    try {
      minio.setBucketTags(SetBucketTagsArgs.builder()
        .bucket(bucket.name)
        .region(params.region ?: region)
        .tags(params.tags.toMap())
        .headers(params.headers, params.tagPutParams.headers)
        .queryParams(params.queryParams, params.tagPutParams.queryParams)
        .build())

      params.tagPutParams.callback?.invoke()
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to put tags onto bucket '$bucket'" }
    }
  }

  private fun getBucket(bucket: BucketName, params: BucketUpsertParams, region: String?, minio: MinioClient): S3Bucket? {
    log.debug("Retrieving bucket '{}'", bucket)

    try {
      return minio.listBuckets(ListBucketsArgs.builder()
        .headers(params.headers, params.getParams.headers)
        .queryParams(params.queryParams, params.getParams.queryParams)
        .build())
        .hunt(bucket, params.region ?: region, minio)
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to get bucket '$bucket'" }
    }
  }
}