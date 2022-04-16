package org.veupathdb.lib.s3.s34k.minio

import io.minio.MinioClient
import io.minio.errors.ErrorResponseException
import io.minio.errors.MinioException
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.*
import org.veupathdb.lib.s3.s34k.errors.S34kException
import org.veupathdb.lib.s3.s34k.params.*
import org.veupathdb.lib.s3.s34k.params.bucket.BucketTagGetParams
import org.veupathdb.lib.s3.s34k.params.`object`.*
import java.io.File
import java.io.InputStream
import java.time.OffsetDateTime

class S3BucketImpl(
  private  val minio: MinioClient,
  override val client: S3Client,
  override val name: String,
  override val region: String?,
  override val creationDate: OffsetDateTime,
) : S3Bucket {

  private val Log = LoggerFactory.getLogger(this::class.java)


  // region: Download Object

  override fun downloadObject(path: String, localFile: File): S3FileObjectImpl {
    Log.trace("downloadObject(path = {}, localFile = {})", path, localFile)

    return downloadObject(ObjectDownloadParams().also {
      it.path = path
      it.localFile = localFile
    })
  }


  override fun downloadObject(action: ObjectDownloadParams.() -> Unit): S3FileObjectImpl {
    Log.trace("downloadObject(action = {})", action)
    return downloadObject(ObjectDownloadParams().also(action))
  }


  override fun downloadObject(params: ObjectDownloadParams): S3FileObjectImpl {
    Log.trace("downloadObject(params = {})", params)

    // Convert the params to a minio config first to do the config validation
    // before we do any file operations.
    val config = params.toMinio()

    // If the file already existed, don't delete it on failure.
    val alreadyExists = params.localFile!!.exists()

    if (!alreadyExists) {
      Log.debug("Local file ${params.localFile} does not exist to download into, creating.")
      params.localFile!!.createNewFile()
    } else {
      Log.debug("Local file ${params.localFile} already exists to download into, it will be truncated.")
    }

    try {
      Log.debug("Attempting to get object {} from bucket {}.", params.path, name)
      val res = minio.getObject(params.toMinio())
      Log.debug("Successfully got object {} from bucket {}.", params.path, name)

      params.localFile!!.outputStream().buffered().use { res.transferTo(it) }

      val out = S3FileObjectImpl(this, res.headers().toMultimap(), params.path, params.localFile)
      params.callback?.invoke(out)

      return out
    } catch (e: MinioException) {

      // If we created this file, delete it to clean up on error.
      if (!alreadyExists)
        params.localFile!!.delete()

      throw S34kException("Failed to download object.", e)
    }
  }

  // endregion


  // region: Get Bucket Tags

  override fun getBucketTags(): S3TagSet {
    Log.trace("getBucketTags()")
    return getBucketTags(BucketTagGetParams())
  }


  override fun getBucketTags(action: BucketTagGetParams.() -> Unit): S3TagSet {
    Log.trace("getBucketTags(action = {})", action)
    return getBucketTags(BucketTagGetParams().also(action))
  }


  override fun getBucketTags(params: BucketTagGetParams): S3TagSet {
    Log.trace("getBucketTags(params = {})", params)

    try {
      val out = S3TagSetImpl(minio.getBucketTags(params.toMinio(name, region)))
      params.callback?.invoke(out)
      return out
    } catch (e: MinioException) {
      throw S34kException("Failed to get bucket tags for bucket $name.", e)
    }
  }

  // endregion


  // region: Get Object

  override fun getObject(path: String): S3StreamObject {
    Log.trace("getObject(path = {})", path)
    return getObject(ObjectGetParams().also { it.path = path })
  }


  override fun getObject(action: ObjectGetParams.() -> Unit): S3StreamObject {
    Log.trace("getObject(action = {})", action)
    return getObject(ObjectGetParams().also(action))
  }


  override fun getObject(params: ObjectGetParams): S3StreamObject {
    Log.trace("getObject(params = {})", params)

    try {
      Log.debug("Attempting to get object {} from bucket {}.", params.path, name)
      val res = minio.getObject(params.toMinio())
      Log.debug("Successfully got object {} from bucket {}.", params.path, name)

      val out = S3StreamObjectImpl(this, res.headers().toMultimap(), params.path, res)
      params.callback?.invoke(out)

      return out
    } catch (e: MinioException) {
      Log.debug("Failed to get object {} from bucket {}.", params.path, name)
      throw S34kException("Failed to get object ${params.path} from bucket $name.", e)
    }
  }

  // endregion

  // region: Get Object Tags

  override fun getObjectTags(path: String): S3TagSet {
    Log.trace("getObjectTags(path = {})", path)
    return getObjectTags(ObjectTagGetParams().also { it.path = path })
  }


  override fun getObjectTags(action: ObjectTagGetParams.() -> Unit): S3TagSet {
    Log.trace("getObjectTags(action = {})", action)
    return getObjectTags(ObjectTagGetParams().also(action))
  }


  override fun getObjectTags(params: ObjectTagGetParams): S3TagSet {
    Log.trace("getObjectTags(params = {})", params)

    try {
      Log.debug("Fetching object tags for {} in bucket {}.", params.path, name)
      val res = minio.getObjectTags(params.toMinio(name, region))
      Log.debug("Successfully fetched object tags for {} in bucket {}.", params.path, name)

      val out = S3TagSetImpl(res)
      params.callback?.invoke(out)

      return out
    } catch (e: MinioException) {
      throw S34kException("Failed to get object tags for ${params.path} from bucket $name.", e)
    }
  }

  // endregion


  // region: Object Exits

  override fun objectExists(path: String): Boolean {
    Log.trace("objectExists(path = {})", path)
    return objectExists(ObjectExistsParams().also { it.path = path })
  }


  override fun objectExists(action: ObjectExistsParams.() -> Unit): Boolean {
    Log.trace("objectExists(action = {})", action)
    return objectExists(ObjectExistsParams().also(action))
  }


  override fun objectExists(params: ObjectExistsParams): Boolean {
    Log.trace("objectExists(params = {})", params)

    try {
      Log.debug("Testing if object {} in bucket {} exists.", params.path, name)
      minio.statObject(params.toMinio(name, region))
      Log.debug("Object {} found in bucket {}.", params.path, name)
      return true
    } catch (e: MinioException) {
      if (e is ErrorResponseException && e.errorResponse().code() == "NoSuchKey") {
        Log.debug("Object {} not found in bucket {}.", params.path, name)
        return false
      }

      Log.debug("Failed to test for object {} existence in bucket {}.", params.path, name)
      throw S34kException("Failed to check for object ${params.path} existence in bucket $name.", e)
    }
  }

  // endregion


  // region: Object Stat

  override fun statObject(path: String): S3ObjectMeta {
    Log.trace("statObject(path = {})", path)
    return statObject(ObjectStatParams().also { it.path = path })
  }


  override fun statObject(action: ObjectStatParams.() -> Unit): S3ObjectMeta {
    Log.trace("statObject(action = {})", action)
    return statObject(ObjectStatParams().also(action))
  }


  override fun statObject(params: ObjectStatParams): S3ObjectMeta {
    Log.trace("statObject(params = {})", params)

    try {
      Log.debug("Attempting to stat object {} in bucket {}.", params.path, name)
      val res = minio.statObject(params.toMinio(name, region))
      Log.debug("Stat for object {} in bucket {} completed successfully", params.path, name)

      val out = S3ObjectMetaImpl(this, region, res)
      params.callback?.invoke(out)

      return out
    } catch (e: MinioException) {
      Log.debug("Stat for object {} in bucket {} failed.", params.path, name)
      throw S34kException("Failed to stat object ${params.path} in bucket $name", e)
    }
  }

  // endregion


  // region: Put Directory

  override fun putDirectory(path: String): S3Object {
    TODO("Not yet implemented")
  }

  override fun putDirectory(params: DirectoryPutParams): S3Object {
    TODO("Not yet implemented")
  }

  override fun putDirectory(action: DirectoryPutParams.() -> Unit): S3Object {
    TODO("Not yet implemented")
  }

  // endregion

  // region: Upload File

  override fun uploadFile(path: String, file: File): S3Object {
    Log.trace("uploadFile(path = {}, file = {})", path, file)
    return uploadFile(ObjectFilePutParams().also {
      it.path = path
      it.localFile = file
    })
  }

  override fun uploadFile(params: ObjectFilePutParams): S3Object {
    Log.trace("uploadFile(params = {})", params)

    try {
      minio.uploadObject()
    }
  }

  override fun uploadFile(action: ObjectFilePutParams.() -> Unit) {
    TODO("Not yet implemented")
  }

  // endregion

  // region: Put Object

  override fun putObject(path: String, stream: InputStream) {
    TODO("Not yet implemented")
  }

  override fun putObject(
    path: String,
    stream: InputStream,
    size: Long,
  ): S3Object {
    TODO("Not yet implemented")
  }

  override fun putObject(params: ObjectPutParams): S3Object {
    TODO("Not yet implemented")
  }

  override fun putObject(action: ObjectPutParams.() -> Unit): S3Object {
    TODO("Not yet implemented")
  }

  // endregion

  // region: Touch Object

  override fun touchObject(path: String): S3Object {
    TODO("Not yet implemented")
  }

  override fun touchObject(params: ObjectTouchParams): S3Object {
    TODO("Not yet implemented")
  }

  override fun touchObject(action: ObjectTouchParams.() -> Unit): S3Object {
    TODO("Not yet implemented")
  }

  // endregion
}