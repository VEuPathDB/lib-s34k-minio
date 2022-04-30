package org.veupathdb.lib.s3.s34k.minio.operations

import io.minio.DeleteObjectTagsArgs
import io.minio.GetObjectTagsArgs
import io.minio.MinioClient
import io.minio.SetObjectTagsArgs
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.core.fields.tags.BasicS3TagMap
import org.veupathdb.lib.s3.s34k.fields.tags.S3TagMap
import org.veupathdb.lib.s3.s34k.minio.invoke
import org.veupathdb.lib.s3.s34k.minio.reqPath
import org.veupathdb.lib.s3.s34k.minio.throwCorrect
import org.veupathdb.lib.s3.s34k.requests.`object`.S3ObjectTagDeleteParams
import org.veupathdb.lib.s3.s34k.response.bucket.S3Bucket

class ObjectTagDeleter(
  private val bucket: S3Bucket,
  private val minio: MinioClient,
  private val params: S3ObjectTagDeleteParams,
) {

  private val Log = LoggerFactory.getLogger(this::class.java)

  /**
   * Performs the targeted tag deletion operations.
   *
   * If the params specify that all tags should be deleted, this method calls
   * [executeAllTags].
   *
   * If the params do not specify that all tags should be deleted, however the
   * target tag list is empty, this method returns without calling anything.
   *
   * If the params do not specify that all tags should be deleted, and the
   * params' target tag list is not empty, this method calls [executePartial].
   *
   * @return Tag map containing only the tags that previously existed on the
   * target object, and have now been deleted.
   */
  fun execute(): S3TagMap {
    Log.trace("execute()")

    Log.debug("Attempting to delete tags from object '{}' in bucket '{}'", params.path, bucket.bucketName)

    // If the caller specified that all tags should be deleted from the target
    // object, execute the simple branch.
    if (params.allTags) {
      Log.debug("All tags flag was specified, attempting to delete all tags from object.")
      return params.callback.invoke("execute", Log, executeAllTags())
    }

    // If the caller did not specify allTags, but also did not target any tags,
    // there's nothing for us to do.
    if (params.tags.isEmpty) {
      Log.debug("Empty tag list provided, no tags to delete, ending here.")
      return params.callback.invoke("execute", Log, BasicS3TagMap())
    }

    // allTags was not specified, and the list of delete targets was not empty.
    Log.debug("Attempting to perform a targeted tag deletion.")
    return params.callback.invoke("execute", Log, executePartial())
  }

  /**
   * Attempts to delete all tags from the target object.
   *
   * Steps:
   * 1. Fetch all tags currently attached to the target object.
   * 2. Delete all tags from the target object.
   * 3. Return the fetched tags.
   *
   * @return Tag map containing all tags that were previously attached to the
   * target object.
   */
  internal fun executeAllTags(): S3TagMap {
    Log.trace("executeAllTags()")

    // Retrieve the list of tags currently attached to the target object.
    val originalTags = fetchTags()

    // Delete all tags from the target object.
    deleteTags()

    // Return a tag map containing all the tags previously attached to the
    // target object.
    return BasicS3TagMap(originalTags)
  }

  /**
   * Attempts to delete the target tags from the target object.
   *
   * @return Tag map containing all tags that were both previously attached to
   * the target object, and appear in the target tag list in the given params.
   */
  internal fun executePartial(): S3TagMap {
    Log.trace("executePartial()")

    val originalTags = fetchTags()

    // If there were no tags attached to the object in the first place, then
    // we can bail here.
    if (originalTags.isEmpty())
      // Since there was nothing that got deleted, return an empty tag map.
      return BasicS3TagMap()

    // Tags that will be re-attached to the object.
    val reAppend = HashMap<String, String>(originalTags.size - params.tags.size)

    // Tags that will be returned to the caller.
    val toReturn = HashMap<String, String>(params.tags.size)

    // Go through all the tags previously attached to the object.  For each tag:
    //
    // * if it appears in the removal list, append it to the tags that will be
    //   returned to the caller
    // * if it does not appear in the removal list, then it is to be re-appended
    originalTags.forEach { (k, v) ->
      if (params.tags.contains(k))
        toReturn[k] = v
      else
        reAppend[k] = v
    }

    // If the toReturn map is empty, then there was no overlap between the
    // tags currently attached to the object, and the list of tags for deletion.
    // This means there was no overlap between target tags and attached tags,
    // and we have nothing to delete.
    //
    // Go ahead and bail here to save the unnecessary operations.
    if (toReturn.isEmpty())
      return BasicS3TagMap()

    // So we do have tags attached to the object that appear in the list of tags
    // to be deleted.
    deleteTags()

    // If we have nothing to re-append to the object, we don't have to try
    // re-appending anything.
    if (reAppend.isNotEmpty())
      appendTags(reAppend)

    return BasicS3TagMap(toReturn)
  }

  /**
   * Attempts to fetch all the tags currently attached to the target object.
   *
   * @return Map of key name to key value for all the keys currently attached to
   * this object.  The map of tags may be empty.
   */
  internal fun fetchTags(): Map<String, String> {
    Log.trace("fetchTags()")

    try {
      Log.debug("Fetching all tags for object '{}' in bucket '{}'", params.path, bucket.bucketName)

      val out = minio.getObjectTags(GetObjectTagsArgs.builder().also {
        it.bucket(bucket.bucketName.name)
        it.region(params.region ?: bucket.defaultRegion)
        it.`object`(params.reqPath())
        // TODO: v0.2.0 version ID
        // TODO: v0.2.0 complex operation headers
        // TODO: v0.2.0 complex operation query params
      }.build()).get()

      Log.debug("Successfully fetched all tags for object '{}' in bucket '{}'", params.path, bucket.bucketName)

      return out
    } catch (e: Throwable) {
      Log.debug("Failed to fetch all tags for object '{}' in bucket '{}'", params.path, bucket.bucketName)

      e.throwCorrect {
        "Failed to fetch all tags for object '${params.path}' in bucket '${bucket.bucketName}'"
      }

      throw IllegalStateException("This will never be reached.")
    }
  }

  /**
   * Attempts to delete all tags from the target object.
   */
  internal fun deleteTags() {
    Log.trace("deleteTags()")

    try {
      Log.debug("Deleting all object tags from object '{}' in bucket '{}'", params.path, bucket.bucketName)

      minio.deleteObjectTags(DeleteObjectTagsArgs.builder().also {
        it.bucket(bucket.bucketName.name)
        it.region(params.region ?: bucket.defaultRegion)
        it.`object`(params.reqPath())
        // TODO: v0.2.0 version ID
        // TODO: v0.2.0 complex operation headers
        // TODO: v0.2.0 complex operation query params
      }.build())

      Log.debug("Successfully deleted all tags from object '{}' in bucket '{}'", params.path, bucket.bucketName)
    } catch (e: Throwable) {
      Log.debug("Failed to delete object tags from object '{}' in bucket '{}'", params.path, bucket.bucketName)

      e.throwCorrect {
        "Failed to delete object tags from object '${params.path}' in bucket '${bucket.bucketName}'"
      }

      throw IllegalStateException("This will never be reached.")
    }
  }

  /**
   * Attempts to append the given map of tags back onto the target object.
   */
  internal fun appendTags(tags: Map<String, String>) {
    Log.trace("appendTags(tags = {})", tags)

    try {
      Log.debug("Re-appending non-target tags to object '{}' in bucket '{}'", params.path, bucket.bucketName)

      minio.setObjectTags(SetObjectTagsArgs.builder().also {
        it.bucket(bucket.bucketName.name)
        it.`object`(params.reqPath())
        it.region(params.region ?: bucket.defaultRegion)
        it.tags(tags)
        // TODO: v0.2.0 version ID
        // TODO: v0.2.0 complex operation headers
        // TODO: v0.2.0 complex operation query params
      }.build())

      Log.debug("Successfully re-appended non-target tags to object '{}' in bucket '{}'", params.path, bucket.bucketName)
    } catch (e: Throwable) {
      Log.debug("Failed to re-append non-target tags to object '{}' in bucket '{}'", params.path, bucket.bucketName)

      e.throwCorrect {
        "Failed to re-append non-target tags to object '${params.path}' in bucket '${bucket.bucketName}'"
      }

      throw IllegalStateException("This will never be reached.")
    }
  }
}