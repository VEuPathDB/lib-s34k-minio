package org.veupathdb.lib.s3.s34k.minio

import io.minio.MinioClient
import io.minio.errors.ErrorResponseException
import io.minio.errors.MinioException
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.*
import org.veupathdb.lib.s3.s34k.params.bucket.BucketName
import org.veupathdb.lib.s3.s34k.params.bucket.BucketTagGetParams
import org.veupathdb.lib.s3.s34k.params.bucket.BucketTagPutParams
import org.veupathdb.lib.s3.s34k.params.`object`.*
import java.io.File
import java.io.InputStream
import java.time.OffsetDateTime

internal class S3BucketImpl(
  private  val minio: MinioClient,
  override val client: S3Client,
  override val name: BucketName,
  override val region: String?,
  override val creationDate: OffsetDateTime,
) : S3Bucket {

  private val Log = LoggerFactory.getLogger(this::class.java)

  // region Download Object

  override fun downloadObject(path: String, localFile: File): S3FileObjectImpl {
    Log.trace("downloadObject(path = {}, localFile = {})", path, localFile)
    return downloadObject(ObjectDownloadParams(path, localFile))
  }

  override fun downloadObject(action: ObjectDownloadParams.() -> Unit): S3FileObjectImpl {
    Log.trace("downloadObject(action = {})", action)
    return downloadObject(ObjectDownloadParams().also(action))
  }

  override fun downloadObject(params: ObjectDownloadParams): S3FileObjectImpl {
    Log.trace("downloadObject(params = {})", params)

    // Convert the params to a minio config first to do the config validation
    // before we do any file operations.
    val config = params.toMinio(name, region)

    // If the file already existed, don't delete it on failure.
    val alreadyExists = params.localFile!!.exists()

    if (!alreadyExists) {
      Log.debug("Local file ${params.localFile} does not exist to download into, creating.")
      params.localFile!!.createNewFile()
    } else {
      Log.debug("Local file ${params.localFile} already exists to download into, it will be truncated.")
    }

    // Attempt the download
    try {
      Log.debug("Attempting to get object {} from bucket {}.", params.path, name)
      val res = minio.getObject(config)
      Log.debug("Successfully got object {} from bucket {}.", params.path, name)

      // Write the S3 object result to the output file.
      params.localFile!!.outputStream().buffered().use { res.copyTo(it) }

      val out = S3FileObjectImpl(this, null, S3HeadersImpl(res.headers()), params.path!!, params.localFile!!)

      // If a callback was set, execute it.
      params.callback?.let {
        Log.debug("Executing action {} in downloadObject", it)
        it.invoke(out)
      }

      return out
    } catch (e: Throwable) {
      Log.debug("Object download failed for object {} in bucket {}", params.path, name)

      // If we created this file, delete it to clean up on error.
      if (!alreadyExists)
        params.localFile!!.delete()

      if (e is MinioException) {
        e.throwCorrect(name, params.path!!) {
          "Object download failed for object ${params.path} in bucket $name"
        }
      }

      throw e
    }
  }

  // endregion

  // region Get Bucket Tags

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
      Log.debug("Attempting to fetch tags for bucket {}", name)
      val out = S3TagSetImpl(minio.getBucketTags(params.toMinio(name, region)))
      Log.debug("Successfully fetched tags for bucket {}", name)

      params.callback?.let {
        Log.trace("Executing action {} getBucketTags", it)
        it.invoke(out)
      }

      return out
    } catch (e: MinioException) {
      Log.debug("Failed to fetch tags for bucket {}", name)
      e.throwCorrect(name) { "Failed to get bucket tags for bucket $name." }
      throw IllegalStateException()
    }
  }

  // endregion

  // region Get Object

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
      val res = minio.getObject(params.toMinio(name, params.path))
      Log.debug("Successfully got object {} from bucket {}.", params.path, name)

      val out = S3StreamObjectImpl(this, region, S3HeadersImpl(res.headers()), params.path!!, res)

      params.callback?.let {
        Log.debug("Executing action {} in getObject", it)
        it.invoke(out)
      }

      return out
    } catch (e: MinioException) {
      Log.debug("Failed to get object {} from bucket {}.", params.path, name)
      e.throwCorrect(name, params.path!!) { "Failed to get object ${params.path} from bucket $name." }
      throw IllegalStateException()
    }
  }

  // endregion

  // region Get Object Tags

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

      params.callback?.let {
        Log.debug("Failed to fetch object tags for {} in bucket {}", params.path, name)
        it.invoke(out)
      }

      return out
    } catch (e: MinioException) {
      Log.debug("Failed to fetch object tags for {} in bucket {}", params.path, name)
      e.throwCorrect(name, params.path!!) { "Failed to get object tags for ${params.path} from bucket $name." }
      throw IllegalStateException()
    }
  }

  // endregion

  // region Object Exits

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
      e.throwCorrect(name, params.path!!) { "Failed to check for object ${params.path} existence in bucket $name." }
      throw IllegalStateException()
    }
  }

  // endregion

  // region Object Stat

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

      params.callback?.let {
        Log.debug("Executing action {} in statObject", it)
        it.invoke(out)
      }

      return out
    } catch (e: MinioException) {
      Log.debug("Stat for object {} in bucket {} failed.", params.path, name)
      e.throwCorrect(name, params.path!!) { "Failed to stat object ${params.path} in bucket $name" }
      throw IllegalStateException()
    }
  }

  // endregion

  // region Put Directory

  override fun putDirectory(path: String): S3Object {
    Log.trace("putDirectory(path = {})", path)
    return putDirectory(DirectoryPutParams(path))
  }

  override fun putDirectory(action: DirectoryPutParams.() -> Unit): S3Object {
    Log.trace("putDirectory(action = {}", action)
    return putDirectory(DirectoryPutParams().also(action))
  }

  override fun putDirectory(params: DirectoryPutParams): S3Object {
    Log.trace("putDirectory(params = {})", params)

    try {
      Log.debug("Attempting to put directory {} into bucket {}", params.path, name)
      val res = minio.putObject(params.toMinio(name, region))
      Log.debug("Successfully put directory {} into bucket {}", params.path, name)

      val out = S3ObjectImpl(this, region, S3HeadersImpl(res.headers()), params.path!!)

      params.callback?.let {
        Log.debug("Executing action {} in putDirectory", it)
        it.invoke(out)
      }

      return out
    } catch (e: MinioException) {
      Log.debug("Failed to put directory {} into bucket {}", params.path, name)
      e.throwCorrect(name, params.path!!) { "Failed to put directory {} into bucket {}" }
      throw IllegalStateException()
    }
  }

  // endregion

  // region Upload File

  override fun uploadFile(path: String, file: File): S3Object {
    Log.trace("uploadFile(path = {}, file = {})", path, file)
    return uploadFile(ObjectFilePutParams(path, file, file.length()))
  }

  override fun uploadFile(action: ObjectFilePutParams.() -> Unit): S3Object {
    Log.trace("uploadFile(action = {})", action)
    return uploadFile(ObjectFilePutParams().also(action))
  }

  override fun uploadFile(params: ObjectFilePutParams): S3Object {
    Log.trace("uploadFile(params = {})", params)

    try {
      Log.debug("Attempting to upload file {} into bucket {} at path {}", params.localFile, name, params.path)
      val res = minio.uploadObject(params.toMinio(name, region))
      Log.debug("Successfully uploaded file {} into bucket {} at path {}", params.localFile, name, params.path)

      val out = S3ObjectImpl(this, res.region(), S3HeadersImpl(res.headers()), params.path!!)

      params.callback?.let {
        Log.debug("Executing action {} in uploadFile", it)
        it.invoke(out)
      }

      return out
    } catch (e: MinioException) {
      Log.debug("Failed to upload file {} into bucket {} at path {}", params.localFile, name, params.path)
      e.throwCorrect(name, params.path!!) {
        "Failed to upload file ${params.localFile} into bucket $name at path ${params.path}"
      }
      throw IllegalStateException()
    }
  }

  // endregion

  // region Put Object

  override fun putObject(path: String, stream: InputStream, size: Long): S3Object {
    Log.trace("putObject(path = {}, stream = ..., size = {}", path, size)
    return putObject(ObjectPutParams(path, stream, size))
  }

  override fun putObject(action: ObjectPutParams.() -> Unit): S3Object {
    Log.trace("putObject(action = {})", action)
    return putObject(ObjectPutParams().also(action))
  }

  override fun putObject(params: ObjectPutParams): S3Object {
    Log.trace("putObject(params = {})", params)

    try {
      Log.debug("Attempting to put object {} into bucket {}", params.path, name)
      val res = minio.putObject(params.toMinio(name, region))
      Log.debug("Successfully put object {} into bucket {}", params.path, name)

      val out = S3ObjectImpl(this, region, S3HeadersImpl(res.headers()), params.path!!)

      params.callback?.let {
        Log.debug("Executing action {} in putObject", it)
        it.invoke(out)
      }

      return out
    } catch (e: MinioException) {
      Log.debug("Failed to put object {} into bucket {}", params.path, name)
      e.throwCorrect(name, params.path!!) { "Failed to put object ${params.path} into bucket $name" }
      throw IllegalStateException()
    }
  }

  // endregion

  // region Touch Object

  override fun touchObject(path: String): S3Object {
    Log.trace("touchObject(path = {})", path)
    return touchObject(ObjectTouchParams(path))
  }

  override fun touchObject(action: ObjectTouchParams.() -> Unit): S3Object {
    Log.trace("touchObject(action = {})", action)
    return touchObject(ObjectTouchParams().also(action))
  }

  override fun touchObject(params: ObjectTouchParams): S3Object {
    Log.trace("touchObject(params = {})", params)

    try {
      Log.debug("Attempting to put empty object {} into bucket {}", params.path, name)
      val res = minio.putObject(params.toMinio(name, region))
      Log.debug("Successfully put empty object {} into bucket {}", params.path, name)

      val out = S3ObjectImpl(this, region, S3HeadersImpl(res.headers()), params.path!!)

      params.callback?.let {
        Log.debug("Executing action {} in touchObject", params.callback)
        it.invoke(out)
      }

      return out
    } catch (e: MinioException) {
      Log.debug("Failed to put empty object {} into bucket {}", params.path, name)
      e.throwCorrect(name, params.path!!) { "Failed to put empty object ${params.path} into bucket $name" }
      throw IllegalStateException()
    }
  }

  // endregion

  // region Put Bucket Tags

  override fun putBucketTags(tags: Collection<S3Tag>) {
    Log.trace("putBucketTags(tags = {})", tags)
    putBucketTags(BucketTagPutParams().also { it.addTags(tags) })
  }

  override fun putBucketTags(tags: Map<String, String>) {
    Log.trace("putBucketTags(tags = {})", tags)
    putBucketTags(BucketTagPutParams().also { it.addTags(tags) })
  }

  override fun putBucketTags(action: BucketTagPutParams.() -> Unit) {
    Log.trace("putBucketTags(action = {})", action)
    putBucketTags(BucketTagPutParams().also(action))
  }

  override fun putBucketTags(params: BucketTagPutParams) {
    Log.trace("putBucketTags(params = {})", params)

    try {
      Log.debug("Attempting to attach tags to bucket {}", name)
      minio.setBucketTags(params.toMinio(name, region))
      Log.debug("Successfully attached tags to bucket {}", name)

      params.callback?.let {
        Log.debug("Executing action {} in putBucketTags", it)
        it.invoke()
      }
    } catch (e: MinioException) {
      Log.debug("Failed to attach tags to bucket {}", name)
      e.throwCorrect(name) { "Failed to attach tags to bucket $name" }
      throw IllegalStateException()
    }
  }

  // endregion

  // region Put Object Tags

  override fun putObjectTags(path: String, tags: Collection<S3Tag>) {
    Log.trace("putObjectTags(path = {}, tags = {})", path, tags)
    return putObjectTags(ObjectTagPutParams(path).also { it.addTags(tags) })
  }


  override fun putObjectTags(path: String, tags: Map<String, String>) {
    Log.trace("putObjectTags(path = {}, tags = {})", path, tags)
    return putObjectTags(ObjectTagPutParams(path))
  }


  override fun putObjectTags(action: ObjectTagPutParams.() -> Unit) {
    Log.trace("putObjectTags(action = {})", action)
    return putObjectTags(ObjectTagPutParams().also(action))
  }


  override fun putObjectTags(params: ObjectTagPutParams) {
    Log.trace("putObjectTags(params = {})", params)

    try {
      if (params.tags.isEmpty()) {
        Log.debug("Tag set empty, skipping")
        return
      }

      Log.debug("Attempting to assign tags to object {} in bucket {}", params.path, name)
      minio.setObjectTags(params.toMinio(name, region))
      Log.debug("Successfully assigned tags to object {} in bucket {}", params.path, name)

      params.callback?.let {
        Log.debug("Executing action {} in putObjectTags", it)
        it.invoke()
      }
    } catch (e: MinioException) {
      Log.debug("Failed to assign tags to object {} in bucket {}", params.path, name)
      e.throwCorrect(name, params.path!!) { "Failed to assign tags to object ${params.path} in bucket $name" }
      throw IllegalStateException()
    }
  }

  // endregion
}