package org.veupathdb.lib.s3.s34k.minio

import io.minio.MinioClient
import io.minio.errors.MinioException
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.S3Bucket
import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.S3Config
import org.veupathdb.lib.s3.s34k.errors.BucketNotFoundException
import org.veupathdb.lib.s3.s34k.errors.S34kException
import org.veupathdb.lib.s3.s34k.params.bucket.BucketExistsParams
import org.veupathdb.lib.s3.s34k.params.bucket.BucketGetParams
import org.veupathdb.lib.s3.s34k.params.bucket.BucketListParams
import org.veupathdb.lib.s3.s34k.params.bucket.BucketPutParams

class S3ClientImpl : S3Client {

  private val Log = LoggerFactory.getLogger(this::class.java)

  private var nullClient: MinioClient? = null

  private inline val client get() = nullClient!!

  override fun initialize(config: S3Config) {
    Log.trace("initialize(config = {})", config)

    if (nullClient != null)
      throw IllegalStateException("S3Client#initialize called more than once.")

    nullClient = MinioClient.builder()
      .region(config.region)
      .endpoint(config.url)
      .credentials(config.accessKey, config.secretKey)
      .build()
  }

  // region: Bucket Exists

  override fun bucketExists(bucketName: String, region: String?): Boolean {
    Log.trace("bucketExists(bucketName = {}, region = {})", bucketName, region)
    return bucketExists(BucketExistsParams().also {
      it.bucket = bucketName
      it.region = region
    })
  }


  override fun bucketExists(action: BucketExistsParams.() -> Unit): Boolean {
    val params = BucketExistsParams()

    action(params)
    return client.bucketExists(params.toMinio())
  }


  override fun bucketExists(params: BucketExistsParams) =
    client.bucketExists(params.toMinio())

  // endregion


  // region: Create Bucket

  override fun createBucket(bucketName: String, region: String?): S3Bucket {
    TODO("Not yet implemented")
  }

  override fun createBucket(params: BucketPutParams): S3Bucket {
    TODO("Not yet implemented")
  }

  override fun createBucket(action: BucketPutParams.() -> Unit): S3Bucket {
    TODO("Not yet implemented")
  }

  // endregion


  // region: Create Bucket if Not Exists

  override fun createBucketIfNotExists(
    bucketName: String,
    region: String?,
  ): S3Bucket {
    TODO("Not yet implemented")
  }

  override fun createBucketIfNotExists(params: BucketPutParams): S3Bucket {
    TODO("Not yet implemented")
  }

  override fun createBucketIfNotExists(action: BucketPutParams.() -> Unit): S3Bucket {
    TODO("Not yet implemented")
  }

  // endregion


  // region: Get Bucket

  override fun getBucket(bucketName: String, region: String?): S3Bucket {
    Log.trace("getBucket(bucketName = {}, region = {})", bucketName, region)

    try {
      Log.debug("Attempting to fetch the list of buckets.")
      val list = client.listBuckets()
      Log.debug("Bucket list successfully fetched.")

      for (b in list) {
        if (b.name() == bucketName) {
          return S3BucketImpl(client, this, bucketName, region, b.creationDate().toOffsetDateTime())
        }
      }

      throw BucketNotFoundException(bucketName)
    } catch (e: MinioException) {
      throw S34kException("Failed to fetch bucket list", e)
    }
  }

  override fun getBucket(action: BucketGetParams.() -> Unit): S3Bucket {
    Log.trace("getBucket(action = {})", action)
    return getBucket(BucketGetParams().also(action))
  }

  override fun getBucket(params: BucketGetParams): S3Bucket {
    Log.trace("getBucket(params = {})", params)

    try {
      Log.debug("Attempting to fetch the list of buckets.")
      val list = client.listBuckets(params.toMinio())
      Log.debug("Bucket list successfully fetched.")

      for (b in list) {
        if (b.name() == params.bucket) {
          return S3BucketImpl(client, this, params.bucket!!, null, b.creationDate().toOffsetDateTime())
        }
      }

      throw BucketNotFoundException(params.bucket!!)
    } catch (e: MinioException) {
      throw S34kException("Failed to fetch bucket list", e)
    }
  }

  // endregion

  override fun listBuckets(): List<S3Bucket> {
    Log.trace("listBuckets()")

    try {
      Log.debug("Attempting to fetch the list of buckets.")
      val list = client.listBuckets()
      Log.debug("Bucket list successfully fetched.")

      return list.map { S3BucketImpl(client, this, it.name(), null, it.creationDate().toOffsetDateTime()) }
    } catch (e: MinioException) {
      throw S34kException("Failed to fetch bucket list", e)
    }
  }

  override fun listBuckets(action: BucketListParams.() -> Unit): List<S3Bucket> {
    Log.trace("listBuckets(action = {})", action)
    return listBuckets(BucketListParams().also(action))
  }

  override fun listBuckets(params: BucketListParams): List<S3Bucket> {
    Log.trace("listBuckets(params = {})", params)

    try {
      Log.debug("Attempting to fetch the list of buckets.")
      val list = client.listBuckets(params.toMinio())
      Log.debug("Bucket list successfully fetched.")

      return list.map { S3BucketImpl(client, this, it.name(), null, it.creationDate().toOffsetDateTime()) }
    } catch (e: MinioException) {
      throw S34kException("Failed to fetch bucket list", e)
    }
  }
}