package org.veupathdb.lib.s3.s34k.minio

import io.minio.BucketExistsArgs
import io.minio.ListBucketsArgs
import io.minio.MakeBucketArgs
import io.minio.MinioClient
import io.minio.RemoveBucketArgs
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.*
import org.veupathdb.lib.s3.s34k.core.BasicS3Client
import org.veupathdb.lib.s3.s34k.core.response.BasicS3BucketList
import org.veupathdb.lib.s3.s34k.errors.*
import org.veupathdb.lib.s3.s34k.fields.BucketName
import org.veupathdb.lib.s3.s34k.minio.operations.RecursiveBucketDeleter
import org.veupathdb.lib.s3.s34k.requests.bucket.recursive.S3ClientRecursiveBucketDeleteParams
import org.veupathdb.lib.s3.s34k.requests.client.*
import org.veupathdb.lib.s3.s34k.response.S3BucketList

internal class MinioS3Client(config: S3Config) : S3Client, BasicS3Client(config.region) {

  private val Log = LoggerFactory.getLogger(this::class.java)

  private val client: MinioClient

  override val defaultRegion: String?

  init {
    Log.trace("::init(config = {})", config)

    // If the port is configured to something other than 0, use that value.
    // If the port is configured to 0, decide the port based on whether we are
    // using https.
    val port = config.port.toInt().let {
      if (it == 0) {
        if (config.secure)
          443
        else
          80
      } else {
        it
      }
    }

    client = MinioClient.builder()
      .region(config.region)
      .endpoint(config.url, port, config.secure)
      .credentials(config.accessKey, config.secretKey)
      .build()

    defaultRegion = config.region
  }


  override fun bucketExists(params: S3BucketExistsParams): Boolean {
    Log.trace("bucketExists(params = {})", params)

    try {
      Log.debug("Attempting to check for the existence of bucket '{}'", params.bucketName)

      val out = client.bucketExists(BucketExistsArgs.builder()
        .bucket(params.reqBucket())
        .regions(params.region, defaultRegion)
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())

      Log.debug("Successfully checked for the existence of bucket '{}', result: {}", params.bucketName, out)

      return params.callback.invoke("bucketExists", Log, out)
    } catch (e: Throwable) {
      Log.debug("Failed to check for bucket '{}' existence.", params.bucketName)

      throw e.toCorrect {
        "Failed to check for bucket ${params.bucketName} existence"
      }
    }
  }


  override fun createBucket(params: S3BucketCreateParams): S3Bucket {
    Log.trace("createBucket(params = {})", params)

    try {
      Log.debug("Attempting to create bucket '{}'", params.bucketName)

      client.makeBucket(MakeBucketArgs.builder()
        .bucket(params.reqBucket())
        .regions(params.region, defaultRegion)
        .headers(params.headers)
        .queryParams(params.queryParams)
        // TODO: v0.2.0 - Object Lock
        .build())

      Log.debug("Successfully created bucket '{}'", params.bucketName)

      return params.callback.invoke("createBucket", Log, getBucket(params.toGetParams()))
    } catch (e: Throwable) {

      Log.debug("Failed to create bucket '{}'", params.bucketName)

      throw e.toCorrect {
        "Failed to check for bucket ${params.bucketName} existence"
      }
    }
  }


  override fun createBucketIfNotExists(params: S3BucketCreateParams): S3Bucket {
    Log.trace("createBucketIfNotExists(params = {})", params)

    try {
      Log.debug("Attempting to create bucket '{}' with exists error catch", params.bucketName)

      client.makeBucket(MakeBucketArgs.builder()
        .bucket(params.reqBucket())
        .regions(params.region, defaultRegion)
        .headers(params.headers)
        .queryParams(params.queryParams)
        // TODO: v0.2.0 - Object Lock
        .build())

      Log.debug("Successfully created bucket '{}' with exists error catch", params.bucketName)

    } catch (e: Throwable) {

      if (e.isBucketCollision()) {
        Log.debug("Bucket '{}' already exists.", params.bucketName)
      } else {
        throw e.toCorrect {
          "Create bucket if not exists failed for bucket '${params.bucketName}'"
        }
      }

    }

    return params.callback.invoke("createBucketIfNotExists", Log, getBucket(params.toGetParams()))
  }


  override fun getBucket(params: S3BucketGetParams): S3Bucket {
    Log.trace("getBucket(params = {})", params)

    try {
      Log.debug("Attempting to fetch the list of buckets.")

      val list = client.listBuckets(ListBucketsArgs.builder()
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())

      Log.debug("Bucket list successfully fetched.")

      Log.debug("Looking for bucket '{}'", params.bucketName)

      for (b in list) {
        if (b.name() == params.bucketName!!.name) {
          Log.debug("Bucket {} found", params.bucketName)
          return params.callback.invoke(
            "getBucket",
            Log,
            MinioS3Bucket(
              client,
              this,
              params.bucketName!!,
              params.region,
              b.creationDate().toOffsetDateTime()
            )
          )
        }
      }

      Log.debug("Bucket '{}' not found", params.bucketName)

      throw BucketNotFoundException(params.bucketName!!)
    } catch (e: Throwable) {
      Log.debug("Failed to fetch bucket list.")
      throw e.toCorrect { "Failed to fetch bucket list" }
    }
  }


  override fun listBuckets(params: S3BucketListParams): S3BucketList {
    Log.trace("listBuckets(params = {})", params)

    try {
      Log.debug("Attempting to fetch the list of buckets.")

      val list = client.listBuckets(ListBucketsArgs.builder()
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())

      Log.debug("Bucket list successfully fetched, got {} buckets.", list.size)

      return params.callback.invoke(
        "listBuckets",
        Log,
        BasicS3BucketList(
          list.map {
            MinioS3Bucket(
              client,
              this,
              BucketName(it.name()),
              null,
              it.creationDate().toOffsetDateTime()
            )
          }
        )
      )
    } catch (e: Throwable) {
      Log.error("Failed to fetch bucket list")

      throw e.toCorrect { "Failed to fetch bucket list" }
    }
  }


  override fun deleteBucket(params: S3BucketDeleteParams): Boolean {
    Log.trace("deleteBucket(params = {})", params)

    try {
      Log.debug("Attempting to delete bucket '{}'", params.bucketName)

      client.removeBucket(RemoveBucketArgs.builder()
        .bucket(params.reqBucket())
        .regions(params.region, defaultRegion)
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())

      Log.debug("Successfully deleted bucket '{}'", params.bucketName)

      return true

    } catch (e: Throwable) {
      if (e.isNoSuchBucket())
        return false

      Log.error("Failed to delete bucket '{}'", params.bucketName)

      throw e.toCorrect {
        "Failed to delete bucket ${params.bucketName}"
      }
    }
  }


  override fun deleteBucketRecursive(params: S3ClientRecursiveBucketDeleteParams): Boolean {
    Log.trace("deleteBucketRecursive(params = {})", params)
    return RecursiveBucketDeleter(this, client, params).execute()
  }
}