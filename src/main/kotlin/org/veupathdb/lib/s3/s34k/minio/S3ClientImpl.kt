package org.veupathdb.lib.s3.s34k.minio

import io.minio.MinioClient
import io.minio.errors.ErrorResponseException
import io.minio.errors.MinioException
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.S3Bucket
import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.S3Config
import org.veupathdb.lib.s3.s34k.S3ErrorCode
import org.veupathdb.lib.s3.s34k.errors.BucketAlreadyExistsException
import org.veupathdb.lib.s3.s34k.errors.BucketNotEmptyException
import org.veupathdb.lib.s3.s34k.errors.BucketNotFoundException
import org.veupathdb.lib.s3.s34k.errors.S34kException
import org.veupathdb.lib.s3.s34k.params.bucket.*

internal class S3ClientImpl(config: S3Config) : S3Client {

  private val Log = LoggerFactory.getLogger(this::class.java)

  private val client: MinioClient

  private val defaultRegion: String?

  init {
    Log.trace("::init(config = {})", config)

    client = MinioClient.builder()
      .region(config.region)
      .endpoint(config.url, 80, config.secure)
      .credentials(config.accessKey, config.secretKey)
      .build()

    defaultRegion = config.region
  }

  // region: Bucket Exists

  override fun bucketExists(name: BucketName, region: String?): Boolean {
    Log.trace("bucketExists(name = {}, region = {})", name, region ?: defaultRegion)
    return bucketExists(BucketExistsParams(null, region).also { it.bucket = name })
  }


  override fun bucketExists(action: BucketExistsParams.() -> Unit): Boolean {
    Log.trace("bucketExists(action = {})", action)
    return bucketExists(BucketExistsParams().also(action))
  }


  override fun bucketExists(params: BucketExistsParams): Boolean {
    Log.trace("bucketExists(params = {})", params)

    try {
      Log.debug("Attempting to check for the existence of bucket {}", params.bucket)

      if (params.region == null)
        params.region = defaultRegion

      val out = client.bucketExists(params.toMinio())
      Log.debug("Successfully checked for the existence of bucket {}, result: {}", params.bucket, out)

      params.callback?.also {
        Log.debug("Executing bucketExists callback {}", it)
        it.invoke(out)
      }


      return out
    } catch (e: MinioException) {
      throw S34kException("Failed to check for bucket {} existence", e)
    }
  }

  // endregion


  // region: Create Bucket

  override fun createBucket(name: BucketName, region: String?): S3Bucket {
    Log.trace("createBucket(name = {}, region = {})", name, region ?: defaultRegion)
    return createBucket(BucketPutParams(null, region).also { it.bucket = name })
  }

  override fun createBucket(action: BucketPutParams.() -> Unit): S3Bucket {
    Log.trace("createBucket(action = {})", action)
    return createBucket(BucketPutParams().also(action))
  }

  override fun createBucket(params: BucketPutParams): S3Bucket {
    Log.trace("createBucket(params = {})", params)

    try {
      Log.debug("Attempting to create bucket {}", params.bucket)

      if (params.region == null)
        params.region = defaultRegion

      client.makeBucket(params.toMinio())
      Log.debug("Successfully created bucket {}", params.bucket)

      val out = getBucket(params.toGetParams())

      params.callback?.let {
        Log.debug("Executing action {} in createBucket", it)
        it.invoke(out)
      }

      return out
    } catch (e: MinioException) {
      if (e is ErrorResponseException && e.errorResponse().code() == S3ErrorCode.BucketAlreadyExists) {
        Log.debug("Bucket {} already exists", params.bucket)
        throw BucketAlreadyExistsException(params.bucket!!, e)
      } else {
        throw S34kException("Failed to create bucket ${params.bucket}", e)
      }
    }

  }

  // endregion


  // region: Create Bucket if Not Exists

  override fun createBucketIfNotExists(name: BucketName, region: String?): S3Bucket {
    Log.trace("createBucketIfNotExists(name = {}, region = {})", name, region ?: defaultRegion)
    return createBucketIfNotExists(BucketPutParams(null, region).also { it.bucket = name })
  }

  override fun createBucketIfNotExists(action: BucketPutParams.() -> Unit): S3Bucket {
    Log.trace("createBucketIfNotExists(action = {})", action)
    return createBucketIfNotExists(BucketPutParams().also(action))
  }

  override fun createBucketIfNotExists(params: BucketPutParams): S3Bucket {
    Log.trace("createBucketIfNotExists(params = {})", params)

    try {
      Log.debug("Attempting to create bucket {} with exists error catch", params.bucket)

      if (params.region == null)
        params.region = defaultRegion

      client.makeBucket(params.toMinio())
      Log.debug("Successfully created bucket {} with exists error catch", params.bucket)

    } catch (e: MinioException) {
      if (e is ErrorResponseException && e.errorResponse().code() == S3ErrorCode.BucketAlreadyExists) {
        Log.debug("Bucket {} already exists with exists error catch", params.bucket)
      } else {
        throw S34kException("Create bucket if not exists failed", e)
      }
    }

    val out = getBucket(params.toGetParams())

    params.callback?.let {
      Log.debug("Executing action {} in createBucketIfNotExists", it)
      it.invoke(out)
    }

    return out
  }

  // endregion


  // region: Get Bucket

  override fun getBucket(name: BucketName, region: String?): S3Bucket {
    Log.trace("getBucket(name = {}, region = {})", name, region ?: defaultRegion)
    return getBucket(BucketGetParams(null, region).also { it.bucket = name })
  }

  override fun getBucket(action: BucketGetParams.() -> Unit): S3Bucket {
    Log.trace("getBucket(action = {})", action)
    return getBucket(BucketGetParams().also(action))
  }

  override fun getBucket(params: BucketGetParams): S3Bucket {
    Log.trace("getBucket(params = {})", params)

    try {
      Log.debug("Attempting to fetch the list of buckets.")

      if (params.region == null)
        params.region = defaultRegion

      val list = client.listBuckets(params.toMinio())
      Log.debug("Bucket list successfully fetched.")

      Log.debug("Looking for bucket {}", params.bucket)
      for (b in list) {
        if (b.name() == params.bucket!!.name) {
          Log.debug("Bucket {} found", params.bucket)
          val out =  S3BucketImpl(
            client,
            this,
            params.bucket!!,
            params.region,
            b.creationDate().toOffsetDateTime()
          )

          if (params.callback != null) {
            Log.debug("Executing action {} in getBucket", params.callback)
            params.callback!!(out)
          }

          return out
        }
      }

      Log.debug("Bucket {} not found", params.bucket)
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
      Log.debug("Bucket list successfully fetched, got {} buckets.", list.size)

      return list.map {
        S3BucketImpl(
          client,
          this,
          BucketName(it.name()),
          null,
          it.creationDate().toOffsetDateTime()
        )
      }
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

      if (params.region == null)
        params.region = defaultRegion

      val list = client.listBuckets(params.toMinio())
      Log.debug("Bucket list successfully fetched, got {} buckets.", list.size)

      val out = list.map {
        S3BucketImpl(
          client,
          this,
          BucketName(it.name()),
          null, it.creationDate().toOffsetDateTime()
        )
      }

      params.callback?.let {
        Log.debug("Executing action {} in listBuckets", it)
        it.invoke(out)
      }

      return out
    } catch (e: MinioException) {
      throw S34kException("Failed to fetch bucket list", e)
    }
  }


  override fun deleteBucket(name: BucketName, region: String?) {
    Log.trace("deleteBucket(name = {}, region = {})", name, region)
    return deleteBucket(BucketDeleteParams(null, region ?: defaultRegion).also { it.bucket = name })
  }

  override fun deleteBucket(action: BucketDeleteParams.() -> Unit) {
    Log.trace("deleteBucket(action = {})", action)
    return deleteBucket(BucketDeleteParams().also(action))
  }

  override fun deleteBucket(params: BucketDeleteParams) {
    Log.trace("deleteBucket(params = {})", params)

    try {
      Log.debug("Attempting to delete bucket ${params.bucket}")
      if (params.region == null) {
        params.region = defaultRegion
      }
      client.removeBucket(params.toMinio())
      Log.debug("Successfully deleted bucket ${params.bucket}")
    } catch (e: MinioException) {
      Log.debug("Failed to delete bucket ${params.bucket}")

      if (e is ErrorResponseException) {
        when (e.errorResponse().code()) {
          S3ErrorCode.NoSuchBucket   -> throw BucketNotFoundException(params.bucket!!, e)
          S3ErrorCode.BucketNotEmpty -> throw BucketNotEmptyException(params.bucket!!, e)
        }
      }

      throw S34kException("Failed to delete bucket ${params.bucket}", e)
    }
  }
}