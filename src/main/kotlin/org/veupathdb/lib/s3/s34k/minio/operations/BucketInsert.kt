package org.veupathdb.lib.s3.s34k.minio.operations

import io.minio.ListBucketsArgs
import io.minio.MakeBucketArgs
import io.minio.MinioClient
import io.minio.SetBucketTagsArgs
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.Bucket
import org.veupathdb.lib.s3.s34k.BucketName
import org.veupathdb.lib.s3.s34k.errors.BucketNotFoundError
import org.veupathdb.lib.s3.s34k.minio.util.*
import org.veupathdb.lib.s3.s34k.params.bucket.put.BucketPutError
import org.veupathdb.lib.s3.s34k.params.bucket.put.BucketPutParams
import org.veupathdb.lib.s3.s34k.params.bucket.put.BucketPutPhase

internal object BucketInsert {

  private val log = LoggerFactory.getLogger(this::class.java)

  fun execute(bucket: BucketName, params: BucketPutParams, region: String?, minio: MinioClient): Bucket {
    putBucket(bucket, params, region, minio)
    putBucketTags(bucket, params, region, minio)

    val out = getBucket(bucket, params, region, minio)
      ?: throw BucketNotFoundError(bucket, "Bucket lookup after creation failed!")

    params.callback?.invoke(out)

    return out
  }

  private fun putBucket(bucket: BucketName, params: BucketPutParams, region: String?, minio: MinioClient) {
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
      throw BucketPutError(bucket, BucketPutPhase.PutBucket, e)
    }
  }

  private fun putBucketTags(bucket: BucketName, params: BucketPutParams, region: String?, minio: MinioClient) {
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
      throw BucketPutError(bucket, BucketPutPhase.PutTags, e)
    }
  }

  private fun getBucket(bucket: BucketName, params: BucketPutParams, region: String?, minio: MinioClient): Bucket? {
    log.debug("Retrieving bucket '{}'", bucket)

    try {
      return minio.listBuckets(ListBucketsArgs.builder()
        .headers(params.headers, params.getParams.headers)
        .queryParams(params.queryParams, params.getParams.queryParams)
        .build())
        .hunt(bucket, params.region ?: region, minio)
    } catch (e: Throwable) {
      throw BucketPutError(bucket, BucketPutPhase.GetBucket, e)
    }
  }
}