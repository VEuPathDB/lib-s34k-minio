package org.veupathdb.lib.s3.s34k.minio

import io.minio.GetBucketTagsArgs
import io.minio.MinioClient
import io.minio.Result
import io.minio.SetBucketTagsArgs
import io.minio.errors.ErrorResponseException
import io.minio.messages.Item
import org.veupathdb.lib.s3.s34k.minio.operations.ObjectTagDeleter
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.*
import org.veupathdb.lib.s3.s34k.core.BasicS3Bucket
import org.veupathdb.lib.s3.s34k.core.fields.tags.BasicS3TagMap
import org.veupathdb.lib.s3.s34k.core.requests.bucket.recursive.BasicS3ClientRecursiveBucketDeleteParams
import org.veupathdb.lib.s3.s34k.core.response.`object`.BasicS3FileObject
import org.veupathdb.lib.s3.s34k.core.response.`object`.BasicS3Object
import org.veupathdb.lib.s3.s34k.core.response.`object`.BasicS3StreamObject
import org.veupathdb.lib.s3.s34k.fields.BucketName
import org.veupathdb.lib.s3.s34k.fields.tags.S3TagMap
import org.veupathdb.lib.s3.s34k.minio.fields.headers.MinioS3Headers
import org.veupathdb.lib.s3.s34k.minio.operations.BucketTagDeleter
import org.veupathdb.lib.s3.s34k.minio.operations.DirectoryDeleter
import org.veupathdb.lib.s3.s34k.minio.response.`object`.MinioS3ObjectList
import org.veupathdb.lib.s3.s34k.minio.response.`object`.MinioS3ObjectMeta
import org.veupathdb.lib.s3.s34k.requests.S3BlankTagCreateParams
import org.veupathdb.lib.s3.s34k.requests.S3BlankTagGetParams
import org.veupathdb.lib.s3.s34k.requests.S3DeleteRequestParams
import org.veupathdb.lib.s3.s34k.requests.bucket.S3BucketTagDeleteParams
import org.veupathdb.lib.s3.s34k.requests.bucket.recursive.S3RecursiveBucketDeleteParams
import org.veupathdb.lib.s3.s34k.requests.`object`.*
import org.veupathdb.lib.s3.s34k.requests.`object`.directory.S3DirectoryCreateParams
import org.veupathdb.lib.s3.s34k.requests.`object`.directory.S3DirectoryDeleteParams
import org.veupathdb.lib.s3.s34k.response.bucket.S3Bucket
import org.veupathdb.lib.s3.s34k.response.`object`.*
import java.time.OffsetDateTime

internal class MinioS3Bucket(
  private val minio: MinioClient,

  client: S3Client,
  bucketName: BucketName,
  defaultRegion: String?,
  creationDate: OffsetDateTime,
) : S3Bucket, BasicS3Bucket(client, bucketName, defaultRegion, creationDate) {

  private val Log = LoggerFactory.getLogger(this::class.java)

  // region Bucket Operations

  override fun getBucketTags(params: S3BlankTagGetParams): S3TagMap {
    Log.trace("getBucketTags(params = {})", params)

    // Attempt to fetch bucket tags.
    try {
      Log.debug("Attempting to fetch tags for bucket '{}'", bucketName)

      val out = BasicS3TagMap(minio.getBucketTags(
        GetBucketTagsArgs.builder()
          .bucket(bucketName.name)
          .regions(params.region, defaultRegion, client.defaultRegion)
          .headers(params.headers)
          .queryParams(params.queryParams)
          .build()).get())

      Log.debug("Successfully fetched {} tags for bucket '{}'", out.size, bucketName)

      return params.callback.invoke("getBucketTags", Log, out)

    } catch (e: Throwable) {
      Log.error("Failed to fetch tags for bucket '{}'", bucketName)
      throw e.toCorrect { "Failed to get bucket tags for bucket $bucketName." }
    }
  }


  override fun putBucketTags(params: S3BlankTagCreateParams) {
    Log.trace("putBucketTags(params = {})", params)

    try {
      Log.debug("Attempting to attach {} tags to bucket '{}'", params.tags.size, bucketName)

      minio.setBucketTags(SetBucketTagsArgs.builder()
        .bucket(bucketName.name)
        .regions(params.region, defaultRegion, client.defaultRegion)
        .headers(params.headers)
        .queryParams(params.queryParams)
        .tags(params.tags.toMap())
        .build())

      Log.debug("Successfully attached {} tags to bucket '{}'", params.tags.size, bucketName)

      params.callback.invoke("putBucketTags", Log)
    } catch (e: Throwable) {
      Log.error("Failed to attach tags to bucket {}", bucketName)
      throw e.toCorrect { "Failed to attach tags to bucket $bucketName" }
    }
  }


  override fun delete(params: S3DeleteRequestParams) {
    Log.trace("delete(params = {})", params)
    return client.deleteBucket(params.toBucketDeleteParams(bucketName))
  }


  override fun deleteRecursive(params: S3RecursiveBucketDeleteParams) {
    Log.trace("deleteRecursive(params = {})", params)

    return client.deleteBucketRecursive(
      BasicS3ClientRecursiveBucketDeleteParams(
        bucketName,
        params))
  }


  override fun deleteBucketTags(params: S3BucketTagDeleteParams): S3TagMap {
    Log.trace("deleteBucketTags(params = {})", params)

    BucketTagDeleter(this, minio, params).execute()

    // If allTags isn't set, and the list of tags to delete is empty, then there
    // is nothing to do.
    if (!params.allTags && params.tags.isEmpty) {
      Log.debug("Short circuiting bucket tag deletion for bucket '{}' as there is no action to perform.",
        bucketName)
      return params.callback.invoke("deleteBucketTags", Log, BasicS3TagMap())
    }

    // So we do have something to do

    // Map of tags currently attached to this bucket
    val oldTags: Map<String, String>

    // Fetch the full list of tags from the store
    try {
      Log.debug("Fetching tag list in preparation for bucket tag deletion on bucket '{}'.",
        bucketName)
      oldTags =
        minio.getBucketTags(params.toBucketTagFetch(bucketName, defaultRegion))
          .get()
      Log.debug("Successfully fetched tag list in preparation for bucket tag deletion on bucket '{}'",
        bucketName)
    } catch (e: Throwable) {
      Log.debug("Failed to fetch tag list in preparation for bucket tag deletion on bucket '{}'",
        bucketName)
      e.throwCorrect { "Failed to fetch tag list in preparation for bucket tag deletion on bucket '$bucketName'" }
      throw IllegalStateException()
    }

    // If there are no tags currently on the bucket, there's nothing more to do.
    if (oldTags.isEmpty()) {
      Log.debug("Short circuiting object tag deletion for bucket '{}' as there are no tags to delete.",
        bucketName)
      return params.callback.invoke("deleteBucketTags", Log, BasicS3TagMap())
    }

    // So we have tags to delete.

    // Run a full delete
    try {
      Log.debug("Deleting tags from bucket '{}'", bucketName)
      minio.deleteBucketTags(params.toBucketTagDelete(bucketName,
        defaultRegion))
      Log.debug("Successfully deleted tags from bucket '{}'", bucketName)
    } catch (e: Throwable) {
      Log.debug("Failed to delete tags from bucket '{}'", bucketName)
      // TODO: If bucket got deleted, what do?
      e.throwCorrect { "Failed to delete tags from bucket '$bucketName'" }
      throw IllegalStateException()
    }

    // If allTags is set, then we are deleting everything and returning
    // everything
    if (params.allTags) {
      Log.debug("All tags flag set, not re-appending any tags to bucket '{}'",
        bucketName)
      return params.callback.invoke("deleteBucketTags",
        Log,
        BasicS3TagMap(oldTags))
    }

    // So we're running a partial delete.

    // Map of tags to return
    val out = HashMap<String, String>(params.tags.size)
    // Filtered map of tags to keep
    val fil = HashMap<String, String>(oldTags.size)

    siftTags(oldTags, params.tags, out, fil)

    // All tags were deleted, there's nothing to re-append.
    if (fil.isEmpty()) {
      Log.debug("No tags re-append to bucket '{}'", bucketName)
      return params.callback.invoke("deleteBucketTags", Log, BasicS3TagMap(out))
    }

    // So we have tags to re-append

    // Attempt to re-append the remaining tags.
    try {
      Log.debug("Re-attaching non-deleted tags to bucket '{}'", bucketName)
      minio.setBucketTags(params.toBucketTagInsert(bucketName,
        defaultRegion,
        fil))
      Log.debug("Successfully re-attached non-deleted tags to bucket '{}'",
        bucketName)
    } catch (e: Throwable) {
      Log.debug("Failed to re-attach tags to bucket '{}'", bucketName)
      // TODO: If bucket got deleted, what do?
      e.throwCorrect { "Failed to re-attach tags to bucket '$bucketName'" }
      throw IllegalStateException()
    }

    return params.callback.invoke("deleteBucketTags", Log, BasicS3TagMap(out))
  }

  // endregion Bucket Operations

  // region Object Operations

  override fun downloadObject(params: S3ObjectDownloadParams): S3FileObject {
    Log.trace("downloadObject(params = {})", params)

    // Convert the params to a minio config first to do the config validation
    // before we do any file operations.
    val config = params.toMinio(bucketName, defaultRegion)

    // If the file already existed, don't delete it on failure.
    val alreadyExists = params.localFile!!.exists()

    if (!alreadyExists) {
      Log.debug("Local file {} does not exist to download into, creating.",
        params.localFile)
      params.localFile!!.createNewFile()
    } else {
      Log.debug("Local file {} already exists to download into, it will be truncated.",
        params.localFile)
    }

    // Attempt the download
    try {
      Log.debug("Attempting to get object {} from bucket {}.",
        params.path,
        bucketName)
      val res = minio.getObject(config)
      Log.debug("Successfully got object {} from bucket {}.",
        params.path,
        bucketName)

      // Write the S3 object result to the output file.
      params.localFile!!.outputStream().buffered().use { res.copyTo(it) }

      return params.callback.invoke(
        "downloadObject",
        Log,
        BasicS3FileObject(
          this,
          MinioS3Headers(res.headers()),
          res.region() ?: defaultRegion,
          params.path!!,
          params.localFile!!
        )
      )
    } catch (e: Throwable) {
      Log.debug("Object download failed for object {} in bucket {}",
        params.path,
        bucketName)

      // If we created this file, delete it to clean up on error.
      if (!alreadyExists)
        params.localFile!!.delete()

      e.throwCorrect {
        "Object download failed for object ${params.path} in bucket $bucketName"
      }

      throw e
    }
  }

  override fun getObject(params: S3ObjectGetParams): S3StreamObject {
    Log.trace("getObject(params = {})", params)

    try {
      Log.debug("Attempting to get object {} from bucket {}.",
        params.path,
        bucketName)
      val res = minio.getObject(params.toMinio(bucketName, params.path))
      Log.debug("Successfully got object {} from bucket {}.",
        params.path,
        bucketName)

      return params.callback.invoke(
        "getObject",
        Log,
        BasicS3StreamObject(
          this,
          MinioS3Headers(res.headers()),
          res.region() ?: defaultRegion,
          params.path!!,
          res
        )
      )
    } catch (e: Throwable) {
      Log.debug("Failed to get object {} from bucket {}.",
        params.path,
        bucketName)
      e.throwCorrect {
        "Failed to get object ${params.path} from bucket $bucketName."
      }
      throw IllegalStateException()
    }
  }

  override fun objectExists(params: S3ObjectExistsParams): Boolean {
    Log.trace("objectExists(params = {})", params)

    try {
      Log.debug("Testing if object {} in bucket {} exists.",
        params.path,
        bucketName)
      minio.statObject(params.toMinio(bucketName, defaultRegion))
      Log.debug("Object {} found in bucket {}.", params.path, bucketName)
      return params.callback.invoke("objectExists", Log, true)
    } catch (e: Throwable) {
      if (e is ErrorResponseException && e.errorResponse()
          .code() == "NoSuchKey"
      ) {
        Log.debug("Object {} not found in bucket {}.", params.path, bucketName)
        return params.callback.invoke("objectExists", Log, false)
      }

      Log.debug("Failed to test for object {} existence in bucket {}.",
        params.path,
        bucketName)
      e.throwCorrect { "Failed to check for object ${params.path} existence in bucket $bucketName." }
      throw IllegalStateException()
    }
  }

  override fun statObject(params: S3ObjectStatParams): S3ObjectMeta {
    Log.trace("statObject(params = {})", params)

    try {
      Log.debug("Attempting to stat object {} in bucket {}.",
        params.path,
        bucketName)
      val res = minio.statObject(params.toMinio(bucketName, defaultRegion))
      Log.debug("Stat for object {} in bucket {} completed successfully",
        params.path,
        bucketName)

      return params.callback.invoke(
        "statObject",
        Log,
        MinioS3ObjectMeta(this, res)
      )
    } catch (e: Throwable) {
      Log.debug("Stat for object {} in bucket {} failed.",
        params.path,
        bucketName)
      e.throwCorrect {
        "Failed to stat object ${params.path} in bucket $bucketName"
      }
      throw IllegalStateException()
    }
  }

  // endregion

  // region Put Directory

  override fun putDirectory(params: S3DirectoryCreateParams): S3Object {
    Log.trace("putDirectory(params = {})", params)

    try {
      Log.debug("Attempting to put directory '{}' into bucket '{}'",
        params.path,
        bucketName)
      val res = minio.putObject(params.toMinio(bucketName, defaultRegion))
      Log.debug("Successfully put directory '{}' into bucket '{}'",
        params.path,
        bucketName)

      return params.callback.invoke(
        "putDirectory",
        Log,
        BasicS3Object(this,
          MinioS3Headers(res.headers()),
          defaultRegion,
          params.path!!)
      )
    } catch (e: Throwable) {
      Log.debug("Failed to put directory '{}' into bucket '{}'",
        params.path,
        bucketName)
      e.throwCorrect {
        "Failed to put directory ${params.path} into bucket $bucketName"
      }
      throw IllegalStateException()
    }
  }

  override fun uploadFile(params: S3FileUploadParams): S3Object {
    Log.trace("uploadFile(params = {})", params)

    try {
      Log.debug("Attempting to upload file '{}' into bucket '{}' at path '{}'",
        params.localFile,
        bucketName,
        params.path)
      val res = minio.uploadObject(params.toMinio(bucketName, defaultRegion))
      Log.debug("Successfully uploaded file '{}' into bucket '{}' at path '{}'",
        params.localFile,
        bucketName,
        params.path)

      return params.callback.invoke(
        "uploadFile",
        Log,
        BasicS3Object(
          this,
          MinioS3Headers(res.headers()),
          res.region(),
          params.path!!
        )
      )
    } catch (e: Throwable) {
      Log.debug("Failed to upload file '{}' into bucket '{}' at path '{}'",
        params.localFile,
        bucketName,
        params.path)
      e.throwCorrect {
        "Failed to upload file ${params.localFile} into bucket $bucketName at path ${params.path}"
      }
      throw IllegalStateException()
    }
  }

  override fun putObject(params: S3StreamingObjectCreateParams): S3Object {
    Log.trace("putObject(params = {})", params)

    try {
      Log.debug("Attempting to put object {} into bucket {}",
        params.path,
        bucketName)
      val res = minio.putObject(params.toMinio(bucketName, defaultRegion))
      Log.debug("Successfully put object {} into bucket {}",
        params.path,
        bucketName)

      return params.callback.invoke(
        "putObject",
        Log,
        BasicS3Object(this,
          MinioS3Headers(res.headers()),
          defaultRegion,
          params.path!!)
      )
    } catch (e: Throwable) {
      Log.debug("Failed to put object {} into bucket {}",
        params.path,
        bucketName)
      e.throwCorrect { "Failed to put object ${params.path} into bucket $bucketName" }
      throw IllegalStateException()
    }
  }

  override fun touchObject(params: S3ObjectTouchParams): S3Object {
    Log.trace("touchObject(params = {})", params)

    try {
      Log.debug("Attempting to put empty object {} into bucket {}",
        params.path,
        bucketName)
      val res = minio.putObject(params.toMinio(bucketName, defaultRegion))
      Log.debug("Successfully put empty object {} into bucket {}",
        params.path,
        bucketName)

      return params.callback.invoke(
        "touchObject",
        Log,
        BasicS3Object(this,
          MinioS3Headers(res.headers()),
          defaultRegion,
          params.path!!)
      )
    } catch (e: Throwable) {
      Log.debug("Failed to put empty object {} into bucket {}",
        params.path,
        bucketName)
      e.throwCorrect { "Failed to put empty object ${params.path} into bucket $bucketName" }
      throw IllegalStateException()
    }
  }

  override fun getObjectTags(params: S3ObjectTagGetParams): S3TagMap {
    Log.trace("getObjectTags(params = {})", params)

    try {
      Log.debug("Fetching object tags for {} in bucket {}.",
        params.path,
        bucketName)
      val res = minio.getObjectTags(params.toMinio(bucketName, defaultRegion))
      Log.debug("Successfully fetched object tags for {} in bucket {}.",
        params.path,
        bucketName)

      return params.callback.invoke("getObjectTags",
        Log,
        BasicS3TagMap(res.get()))
    } catch (e: Throwable) {
      Log.debug("Failed to fetch object tags for {} in bucket {}",
        params.path,
        bucketName)
      e.throwCorrect { "Failed to get object tags for ${params.path} from bucket $bucketName." }
      throw IllegalStateException()
    }
  }

  override fun putObjectTags(params: S3ObjectTagCreateParams) {
    Log.trace("putObjectTags(params = {})", params)

    try {
      if (params.tags.isEmpty) {
        Log.debug("Tag set empty, skipping")
        return params.callback.invoke("putObjectTags", Log)
      }

      Log.debug("Attempting to assign tags to object {} in bucket {}",
        params.path,
        bucketName)
      minio.setObjectTags(params.toMinio(bucketName, defaultRegion))
      Log.debug("Successfully assigned tags to object {} in bucket {}",
        params.path,
        bucketName)

      params.callback.invoke("putObjectTags", Log)
    } catch (e: Throwable) {
      Log.debug("Failed to assign tags to object {} in bucket {}",
        params.path,
        bucketName)
      e.throwCorrect { "Failed to assign tags to object ${params.path} in bucket $bucketName" }
      throw IllegalStateException()
    }
  }

  override fun deleteObjectTags(params: S3ObjectTagDeleteParams): S3TagMap {
    Log.trace("deleteObjectTags(params = {})", params)
    return ObjectTagDeleter(this, minio, params).execute()
  }

  override fun deleteObject(params: S3ObjectDeleteParams) {
    Log.trace("deleteObject(params = {})", params)

    try {
      Log.debug("Attempting to delete object '{}' from bucket '{}'",
        params.path,
        bucketName)
      minio.removeObject(params.toMinio(bucketName, defaultRegion))
      Log.debug("Successfully deleted object '{}' from bucket '{}'",
        params.path,
        bucketName)

      return
    } catch (e: Throwable) {
      if (e is ErrorResponseException && e.errorResponse()
          .code() == S3ErrorCode.NoSuchKey
      ) {
        Log.debug("Could not delete object '{}' from bucket '{}'.  Object did not exist.",
          params.path,
          bucketName)
        return
      }

      Log.error("Failed to delete object '{}' from bucket '{}'",
        params.path,
        bucketName)
      throw e.toCorrect {
        "Failed to delete object '${params.path}' from bucket '$bucketName'"
      }
    }
  }

  override fun deleteObjects(params: S3MultiObjectDeleteParams) {
    Log.trace("deleteObjects(params = {})", params)

    try {
      Log.debug("Attempting to delete specified objects from bucket '{}'",
        bucketName)

      minio.removeObjects(params.toMinio(bucketName, defaultRegion))

      Log.debug("Successfully deleted specified objects from bucket '{}'",
        bucketName)
    } catch (e: Throwable) {
      Log.error("Failed to delete specified objects from bucket '{}'",
        bucketName)
      e.throwCorrect {
        "Failed to delete specified objects from bucket '$bucketName'"
      }
      throw IllegalStateException()
    }
  }

  override fun listObjects(params: S3ObjectListParams): S3ObjectList {
    Log.trace("listObjects(params = {})", params)

    val objects: Iterable<Result<Item>>

    try {
      Log.debug("Fetching object list for bucket '{}'", bucketName)

      objects = minio.listObjects(params.toMinio(bucketName, defaultRegion))

      Log.debug("Successfully fetched object list for bucket '{}'", bucketName)
    } catch (e: Throwable) {
      Log.error("Failed to fetch object list for bucket '{}'", bucketName)
      e.throwCorrect {
        "Failed to fetch object list for bucket $bucketName"
      }
      throw IllegalStateException()
    }

    return params.callback.invoke(
      "listObjects",
      Log,
      MinioS3ObjectList(this, objects)
    )
  }

  override fun deleteDirectory(params: S3DirectoryDeleteParams) {
    Log.trace("deleteDirectory(params = {})", params)

    return DirectoryDeleter(minio, this, params).execute()
  }

  // endregion Object Operations

  // region Utilities

  private fun siftTags(
    original: Map<String, String>,
    tags: Iterable<String>,
    deleted: MutableMap<String, String>,
    keep: MutableMap<String, String>,
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