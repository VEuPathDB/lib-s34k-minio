package org.veupathdb.lib.s3.s34k.minio.operations

import io.minio.DeleteObjectTagsArgs
import io.minio.GetObjectTagsArgs
import io.minio.MinioClient
import io.minio.SetObjectTagsArgs
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.S3Object
import org.veupathdb.lib.s3.s34k.minio.util.bucket
import org.veupathdb.lib.s3.s34k.minio.util.headers
import org.veupathdb.lib.s3.s34k.minio.util.queryParams
import org.veupathdb.lib.s3.s34k.minio.util.region
import org.veupathdb.lib.s3.s34k.params.tag.TagDeleteParams
import org.veupathdb.lib.s3.s34k.params.tag.TargetedTagDeleteError
import org.veupathdb.lib.s3.s34k.params.tag.TargetedTagDeletePhase

internal class ObjectTagDeleter(
  private val handle: S3Object,
  private val client: MinioClient,
  private val params: TagDeleteParams,
) {

  private val log = LoggerFactory.getLogger(this::class.java)

  fun execute() {
    // If the caller didn't specify any tags to delete, then we have nothing to
    // do.  Bail here.
    if (params.tags.isEmpty) {
      params.callback?.invoke()
      return
    }

    // Get all the tags currently attached to the target object.
    val original = fetch()

    // Post fetch callback
    params.fetchParams.callback?.invoke()

    // If there were no tags attached to the object in the first place, then
    // we have nothing to delete.  Bail here.
    if (original.isEmpty()) {
      params.callback?.invoke()
      return
    }

    // counter of tags that occur in both the params' tag set and the tags
    // currently attached to the target object.
    var overlap = 0

    // map of tags that exist on the target object, but do not exist in the
    // params' tag set.
    val keep = HashMap<String, String>(10)

    // Sift through the tags to determine what tags we are keeping.
    original.forEach { (k, v) ->
      // If the key in the tags currently attached to the object matches a
      // tag targeted for deletion, increment the overlap counter to indicate
      // that we have something to delete.
      if (k in params.tags)
        overlap++
      // If the key in the tags currently attached to the object does not
      // match any tags targeted for deletion, add it to the keep map.
      else
        keep[k] = v
    }

    // If there is no overlap between the tags currently attached to the
    // object and the tags we want to delete, then there is nothing to do.
    if (overlap == 0) {
      params.callback?.invoke()
      return
    }

    // So we do have tags to delete.

    // Delete all the tags currently attached to the object (S3 offers no
    // function for targeted tag deletion).
    delete()

    // post delete callback
    params.deleteParams.callback?.invoke()

    // If we don't have any tags that we want to keep, then we have nothing
    // more to do.  Bail here.
    if (keep.isEmpty()) {
      params.callback?.invoke()
      return
    }

    // So we have tags to re-attach to the target object.

    // Re-attach the tags to keep to the target object.
    put(keep)

    // Post put callback
    params.putParams.callback?.invoke()

    // operation end callback
    params.callback?.invoke()
  }

  private fun fetch(): Map<String, String> {
    log.debug("Fetching tags for object '{}' in bucket '{}'", handle.path, handle.bucket.name)

    try {
      return client.getObjectTags(GetObjectTagsArgs.builder()
        .bucket(handle.bucket)
        .region(params, handle)
        .`object`(handle.path)
        .headers(params.headers, params.fetchParams.headers)
        .queryParams(params.queryParams, params.fetchParams.queryParams)
        // TODO: Version ID
        .build()).get()
    } catch (e: Throwable) {
      throw TargetedTagDeleteError(handle, TargetedTagDeletePhase.Get, params, e)
    }
  }

  private fun delete() {
    log.debug("Deleting all tags from object '{}' in bucket '{}'", handle.path, handle.bucket.name)

    try {
      client.deleteObjectTags(DeleteObjectTagsArgs.builder()
        .bucket(handle.bucket)
        .region(params, handle)
        .`object`(handle.path)
        .headers(params.headers, params.deleteParams.headers)
        .queryParams(params.queryParams, params.deleteParams.queryParams)
        // TODO: Version ID
        .build())
    } catch (e: Throwable) {
      throw TargetedTagDeleteError(handle, TargetedTagDeletePhase.Delete, params, e)
    }
  }

  private fun put(tags: Map<String, String>) {
    log.debug("Re-attaching {} tags to object '{}' in bucket '{}'", tags.size, handle.path, handle.bucket.name)

    try {
      client.setObjectTags(SetObjectTagsArgs.builder()
        .bucket(handle.bucket)
        .region(params, handle)
        .`object`(handle.path)
        .headers(params.headers, params.putParams.headers)
        .queryParams(params.queryParams, params.putParams.queryParams)
        // TODO: Version ID
        .build())
    } catch (e: Throwable) {
      throw TargetedTagDeleteError(handle, TargetedTagDeletePhase.Put, params, e)
    }
  }
}