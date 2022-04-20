package org.veupathdb.lib.s3.s34k.minio

import io.minio.MinioClient
import io.minio.errors.ErrorResponseException
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.*
import org.veupathdb.lib.s3.s34k.params.DeleteParams
import org.veupathdb.lib.s3.s34k.params.bucket.BucketName
import org.veupathdb.lib.s3.s34k.params.bucket.BucketTagDeleteParams
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

  // region Bucket Operations

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

    // Attempt to fetch bucket tags.
    try {
      Log.debug("Attempting to fetch tags for bucket {}", name)
      val out = S3TagSetImpl(minio.getBucketTags(params.toMinio(name, region)))
      Log.debug("Successfully fetched tags for bucket {}", name)
      params.callback.invoke("getBucketTags", Log, out)
      return out
    } catch (e: Throwable) {
      Log.debug("Failed to fetch tags for bucket {}", name)
      e.throwCorrect(name) { "Failed to get bucket tags for bucket $name." }
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
      params.callback.invoke("putBucketTags", Log)
    } catch (e: Throwable) {
      Log.debug("Failed to attach tags to bucket {}", name)
      e.throwCorrect(name) { "Failed to attach tags to bucket $name" }
      throw IllegalStateException()
    }
  }

  // endregion

  // region Delete

  override fun delete() {
    Log.trace("delete()")
    client.deleteBucket(name, region)
  }

  override fun delete(action: DeleteParams.() -> Unit) {
    Log.trace("delete(action = {})", action)
    delete(DeleteParams().also(action))
  }

  override fun delete(params: DeleteParams) {
    Log.trace("delete(params = {})", params)
    client.deleteBucket(params.toBucketDeleteParams(name, region))
  }

  // endregion

  // region Delete Bucket Tags

  override fun deleteBucketTags(): S3TagSet {
    Log.trace("deleteBucketTags()")
    return deleteBucketTags(BucketTagDeleteParams().also { it.allTags = true })
  }

  override fun deleteBucketTags(vararg tags: String): S3TagSet {
    Log.trace("deleteBucketTags(tags = {})", tags)
    return deleteBucketTags(BucketTagDeleteParams().also { it.addTags(tags.asList()) })
  }

  override fun deleteBucketTags(tags: Iterable<String>): S3TagSet {
    Log.trace("deleteBucketTags(tags = {})", tags)
    return deleteBucketTags(BucketTagDeleteParams().also { it.addTags(tags) })
  }

  override fun deleteBucketTags(action: BucketTagDeleteParams.() -> Unit): S3TagSet {
    Log.trace("deleteBucketTags(action = {})", action)
    return deleteBucketTags(BucketTagDeleteParams().also(action))
  }

  override fun deleteBucketTags(params: BucketTagDeleteParams): S3TagSet {
    Log.trace("deleteBucketTags(params = {})", params)

    // If allTags isn't set, and the list of tags to delete is empty, then there
    // is nothing to do.
    if (!params.allTags && params.tags.isEmpty()) {
      Log.debug("Short circuiting bucket tag deletion for bucket '{}' as there is no action to perform.", name)
      return params.callback.invoke("deleteBucketTags", Log, S3TagSetImpl(emptyMap()))
    }

    // So we do have something to do

    // Map of tags currently attached to this bucket
    val oldTags: Map<String, String>

    // Fetch the full list of tags from the store
    try {
      Log.debug("Fetching tag list in preparation for bucket tag deletion on bucket '{}'.", name)
      oldTags = minio.getBucketTags(params.toBucketTagFetch(name, region)).get()
      Log.debug("Successfully fetched tag list in preparation for bucket tag deletion on bucket '{}'", name)
    } catch (e: Throwable) {
      Log.debug("Failed to fetch tag list in preparation for bucket tag deletion on bucket '{}'", name)
      e.throwCorrect(name) { "Failed to fetch tag list in preparation for bucket tag deletion on bucket '$name'" }
      throw IllegalStateException()
    }

    // If there are no tags currently on the bucket, there's nothing more to do.
    if (oldTags.isEmpty()) {
      Log.debug("Short circuiting object tag deletion for bucket '{}' as there are no tags to delete.", name)
      return params.callback.invoke("deleteBucketTags", Log, S3TagSetImpl(emptyMap()))
    }

    // So we have tags to delete.

    // Run a full delete
    try {
      Log.debug("Deleting tags from bucket '{}'", name)
      minio.deleteBucketTags(params.toBucketTagDelete(name, region))
      Log.debug("Successfully deleted tags from bucket '{}'", name)
    } catch (e: Throwable) {
      Log.debug("Failed to delete tags from bucket '{}'", name)
      // TODO: If bucket got deleted, what do?
      e.throwCorrect(name) { "Failed to delete tags from bucket '$name'" }
      throw IllegalStateException()
    }

    // If allTags is set, then we are deleting everything and returning
    // everything
    if (params.allTags) {
      Log.debug("All tags flag set, not re-appending any tags to bucket '{}'", name)
      return params.callback.invoke("deleteBucketTags", Log, S3TagSetImpl(oldTags))
    }

    // So we're running a partial delete.

    // Map of tags to return
    val out = HashMap<String, String>(params.tags.size)
    // Filtered map of tags to keep
    val fil = HashMap<String, String>(oldTags.size)

    siftTags(oldTags, params.tags, out, fil)

    // All tags were deleted, there's nothing to re-append.
    if (fil.isEmpty()) {
      Log.debug("No tags re-append to bucket '{}'", name)
      return params.callback.invoke("deleteBucketTags", Log, S3TagSetImpl(out))
    }

    // So we have tags to re-append

    // Attempt to re-append the remaining tags.
    try {
      Log.debug("Re-attaching non-deleted tags to bucket '{}'", name)
      minio.setBucketTags(params.toBucketTagInsert(name, region, fil))
      Log.debug("Successfully re-attached non-deleted tags to bucket '{}'", name)
    } catch (e: Throwable) {
      Log.debug("Failed to re-attach tags to bucket '{}'", name)
      // TODO: If bucket got deleted, what do?
      e.throwCorrect(name) { "Failed to re-attach tags to bucket '$name'" }
      throw IllegalStateException()
    }

    return params.callback.invoke("deleteBucketTags", Log, S3TagSetImpl(out))
  }

  // endregion Delete Bucket Tags

  // endregion Bucket Operations

  // region Object Operations

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
      Log.debug("Local file {} does not exist to download into, creating.", params.localFile)
      params.localFile!!.createNewFile()
    } else {
      Log.debug("Local file {} already exists to download into, it will be truncated.", params.localFile)
    }

    // Attempt the download
    try {
      Log.debug("Attempting to get object {} from bucket {}.", params.path, name)
      val res = minio.getObject(config)
      Log.debug("Successfully got object {} from bucket {}.", params.path, name)

      // Write the S3 object result to the output file.
      params.localFile!!.outputStream().buffered().use { res.copyTo(it) }

      return params.callback.invoke(
        "downloadObject",
        Log,
        S3FileObjectImpl(
          this,
          null,
          S3HeadersImpl(res.headers()),
          params.path!!,
          params.localFile!!
        )
      )
    } catch (e: Throwable) {
      Log.debug("Object download failed for object {} in bucket {}", params.path, name)

      // If we created this file, delete it to clean up on error.
      if (!alreadyExists)
        params.localFile!!.delete()

      e.throwCorrect(name, params.path!!) {
        "Object download failed for object ${params.path} in bucket $name"
      }

      throw e
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

      return params.callback.invoke(
        "getObject",
        Log,
        S3StreamObjectImpl(
          this,
          region,
          S3HeadersImpl(res.headers()),
          params.path!!,
          res
        )
      )
    } catch (e: Throwable) {
      Log.debug("Failed to get object {} from bucket {}.", params.path, name)
      e.throwCorrect(name, params.path!!) {
        "Failed to get object ${params.path} from bucket $name."
      }
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
      return params.callback.invoke("objectExists", Log, true)
    } catch (e: Throwable) {
      if (e is ErrorResponseException && e.errorResponse().code() == "NoSuchKey") {
        Log.debug("Object {} not found in bucket {}.", params.path, name)
        return params.callback.invoke("objectExists", Log, false)
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

      return params.callback.invoke(
        "statObject",
        Log,
        S3ObjectMetaImpl(this, region, res)
      )
    } catch (e: Throwable) {
      Log.debug("Stat for object {} in bucket {} failed.", params.path, name)
      e.throwCorrect(name, params.path!!) {
        "Failed to stat object ${params.path} in bucket $name"
      }
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
      Log.debug("Attempting to put directory '{}' into bucket '{}'", params.path, name)
      val res = minio.putObject(params.toMinio(name, region))
      Log.debug("Successfully put directory '{}' into bucket '{}'", params.path, name)

      return params.callback.invoke(
        "putDirectory",
        Log,
        S3ObjectImpl(this, region, S3HeadersImpl(res.headers()), params.path!!)
      )
    } catch (e: Throwable) {
      Log.debug("Failed to put directory '{}' into bucket '{}'", params.path, name)
      e.throwCorrect(name, params.path!!) {
        "Failed to put directory ${params.path} into bucket $name"
      }
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
      Log.debug("Attempting to upload file '{}' into bucket '{}' at path '{}'", params.localFile, name, params.path)
      val res = minio.uploadObject(params.toMinio(name, region))
      Log.debug("Successfully uploaded file '{}' into bucket '{}' at path '{}'", params.localFile, name, params.path)

      return params.callback.invoke(
        "uploadFile",
        Log,
        S3ObjectImpl(
          this,
          res.region(),
          S3HeadersImpl(res.headers()),
          params.path!!
        )
      )
    } catch (e: Throwable) {
      Log.debug("Failed to upload file '{}' into bucket '{}' at path '{}'", params.localFile, name, params.path)
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

      return params.callback.invoke(
        "putObject",
        Log,
        S3ObjectImpl(this, region, S3HeadersImpl(res.headers()), params.path!!)
      )
    } catch (e: Throwable) {
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

      return params.callback.invoke(
        "touchObject",
        Log,
        S3ObjectImpl(this, region, S3HeadersImpl(res.headers()), params.path!!)
      )
    } catch (e: Throwable) {
      Log.debug("Failed to put empty object {} into bucket {}", params.path, name)
      e.throwCorrect(name, params.path!!) { "Failed to put empty object ${params.path} into bucket $name" }
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

      return params.callback.invoke("getObjectTags", Log, S3TagSetImpl(res))
    } catch (e: Throwable) {
      Log.debug("Failed to fetch object tags for {} in bucket {}", params.path, name)
      e.throwCorrect(name, params.path!!) { "Failed to get object tags for ${params.path} from bucket $name." }
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
        return params.callback.invoke("putObjectTags", Log)
      }

      Log.debug("Attempting to assign tags to object {} in bucket {}", params.path, name)
      minio.setObjectTags(params.toMinio(name, region))
      Log.debug("Successfully assigned tags to object {} in bucket {}", params.path, name)

      params.callback.invoke("putObjectTags", Log)
    } catch (e: Throwable) {
      Log.debug("Failed to assign tags to object {} in bucket {}", params.path, name)
      e.throwCorrect(name, params.path!!) { "Failed to assign tags to object ${params.path} in bucket $name" }
      throw IllegalStateException()
    }
  }

  // endregion

  // region Delete Object Tags
  override fun deleteObjectTags(path: String): S3TagSet {
    Log.trace("deleteObjectTags(path = {})", path)

    // Map of tags originally assigned to the target object.
    val oldTags: Map<String, String>

    // Fetch the tags currently assigned to the target object.
    try {
      Log.debug("Fetching tags for object '{}' in bucket '{}'", path, name)
      oldTags = minio.getObjectTags(path.toObjectTagFetch(name, region)).get()
      Log.debug("Successfully fetched tags for object '{}' in bucket '{}'", path, name)
    } catch (e: Throwable) {
      Log.debug("Failed to fetch tags for object '{}' in bucket '{}'", path, name)
      e.throwCorrect(name, path) {
        "Failed to fetch tags for object '$path' in bucket '$name'"
      }
      throw IllegalStateException()
    }

    // If there were no tags originally assigned to the object, there is nothing
    // more to do.
    if (oldTags.isEmpty())
      return S3TagSetImpl(oldTags)

    // We've fetched 1 or more original tags from the object, now delete
    // everything.
    try {
      Log.debug("Deleting all tags from object '{}' in bucket '{}'", path, name)
      minio.deleteObjectTags(path.toObjectTagDelete(name, region))
      Log.debug("Successfully deleted all tags from object '{}' in bucket '{}'", path, name)
    } catch (e: Throwable) {
      Log.debug("Failed to delete all tags from object '{}' in bucket '{}'", path, name)
      e.throwCorrect(name, path) {
        "Failed to delete all tags from object '$path' in bucket '$name'"
      }
      throw IllegalStateException()
    }

    return S3TagSetImpl(oldTags)
  }

  override fun deleteObjectTags(path: String, vararg tags: String): S3TagSet {
    Log.trace("deleteObjectTags(path = {}, tags = {})", path, tags)
    return deleteObjectTags(ObjectTagDeleteParams(path).also { it.addTags(tags.asList()) })
  }

  override fun deleteObjectTags(path: String, tags: Iterable<String>): S3TagSet {
    Log.trace("deleteObjectTags(path = {}, tags = {})", path, tags)
    return deleteObjectTags(ObjectTagDeleteParams(path).also { it.addTags(tags) })
  }

  override fun deleteObjectTags(action: ObjectTagDeleteParams.() -> Unit): S3TagSet {
    Log.trace("deleteObjectTags(action = {})", action)
    return deleteObjectTags(ObjectTagDeleteParams().also(action))
  }

  override fun deleteObjectTags(params: ObjectTagDeleteParams): S3TagSet {
    Log.trace("deleteObjectTags(params = {})", params)

    // If the caller has not specified that all tags must be deleted, and the
    // set of target tags is empty, then there is nothing for us to do.
    if (!params.allTags && params.tags.isEmpty()) {
      Log.debug("Short circuiting object tag deletion for object '{}' in bucket '{}' as there is no action to perform.", params.path, name)
      return params.callback.invoke("deleteObjectTags", Log, S3TagSetImpl(emptyMap()))
    }

    // Map of tags currently assigned to the object
    val oldTags: Map<String, String>

    // Fetch the tags currently assigned to the object
    try {
      Log.debug("Fetching tags for object '{}' in bucket '{}'", params.path, name)
      oldTags = minio.getObjectTags(params.toObjectTagFetch(name, region)).get()
      Log.debug("Successfully fetched tags for object '{}' in bucket '{}'", params.path, name)
    } catch (e: Throwable) {
      Log.debug("Failed to fetch tags for object '{}' in bucket '{}'", params.path, name)
      e.throwCorrect(name, params.path!!) { "Failed to fetch tags for object '${params.path}' in bucket '$name'" }
      throw IllegalStateException()
    }

    // If there are no tags currently on the object, there is nothing more to
    // do.
    if (oldTags.isEmpty()) {
      Log.debug("Short circuiting object tag deletion for object '{}' in bucket '{}' as there are no tags to delete.", params.path, name)
      return params.callback.invoke("deleteObjectTags", Log, S3TagSetImpl(emptyMap()))
    }

    // So we have tags to delete.

    // Delete all tags from the object
    try {
      Log.debug("Deleting tags from object '{}' in bucket '{}'", params.path, name)
      minio.deleteObjectTags(params.toObjectTagDelete(name, region))
      Log.debug("Successfully deleted tags from object '{}' in bucket '{}'", params.path, name)
    } catch (e: Throwable) {
      Log.error("Failed to delete tags from object '{}' in bucket '{}'", params.path, name)
      e.throwCorrect(name, params.path!!) { "Failed to delete tags from object '${params.path}' in bucket '$name'" }
      throw IllegalStateException()
    }

    // If 'allTags' was set, then we are not re-appending anything, bail here.
    if (params.allTags) {
      Log.debug("'allTags' flag set, not re-appending tags to object '{}' in bucket '{}'", params.path, name)
      return params.callback.invoke("deleteObjectTags", Log, S3TagSetImpl(oldTags))
    }

    // So we have tags to re-append

    // Map containing the tags that were deleted
    val deleted = HashMap<String, String>(params.tags.size)
    // Map containing remaining tags to re-append
    val reAppend = HashMap<String, String>(oldTags.size)

    siftTags(oldTags, params.tags, deleted, reAppend)

    // If all tags were deleted, there is nothing to re-append
    if (reAppend.isEmpty()) {
      Log.debug("No tags re-append to object '{}' in bucket '{}'", params.path, name)
      return params.callback.invoke("deleteBucketTags", Log, S3TagSetImpl(deleted))
    }

    // Attempt to re-append the non-deleted tags.
    try {
      Log.debug("Re-appending tags to object '{}' in bucket '{}'", params.path, name)
      minio.setObjectTags(params.toObjectTagPut(name, region, reAppend))
      Log.debug("Successfully re-appended tags to object '{}' in bucket '{}'", params.path, name)
    } catch (e: Throwable) {
      Log.debug("Failed to re-append tags to object '{}' in bucket '{}'", params.path, name)
      e.throwCorrect(name, params.path!!) {
        "Failed to re-append tags to object '${params.path}' in bucket '$name'"
      }
      throw IllegalStateException()
    }

    return params.callback.invoke("deleteObjectTags", Log, S3TagSetImpl(deleted))
  }

  // endregion Delete Object Tags

  // endregion Object Operations

  // region Utilities

  private fun siftTags(
    original: Map<String, String>,
    tags:     Iterable<String>,
    deleted:  MutableMap<String, String>,
    keep:     MutableMap<String, String>,
  ) {

    // For each tag in the tag deletion list, if that tag exists in the map of
    // original tags, add the tag to the deleted map.
    tags.forEach {
      val value = original[it]

      if (value != null) {
        deleted[it] = value
      }
    }

    // For each pair in the original tag map, if that tag DOES NOT exist in the
    // deleted map, append it to the keep map.
    original.forEach { (k, v) -> if (!deleted.containsKey(k)) keep[k] = v }
  }

  // endregion Utilities
}