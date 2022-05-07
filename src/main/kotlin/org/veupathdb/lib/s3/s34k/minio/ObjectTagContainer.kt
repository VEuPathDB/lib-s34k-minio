package org.veupathdb.lib.s3.s34k.minio

import io.minio.DeleteObjectTagsArgs
import io.minio.GetObjectTagsArgs
import io.minio.MinioClient
import io.minio.SetObjectTagsArgs
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.S3Object
import org.veupathdb.lib.s3.s34k.core.AbstractObjectTagContainer
import org.veupathdb.lib.s3.s34k.core.fields.BasicTagMap
import org.veupathdb.lib.s3.s34k.fields.TagMap
import org.veupathdb.lib.s3.s34k.minio.operations.ObjectTagDeleter
import org.veupathdb.lib.s3.s34k.minio.util.*
import org.veupathdb.lib.s3.s34k.params.RegionRequestParams
import org.veupathdb.lib.s3.s34k.params.tag.*

internal class ObjectTagContainer(
  private val handle: S3Object,
  private val client: MinioClient,
) : AbstractObjectTagContainer() {

  private val log = LoggerFactory.getLogger(this::class.java)

  override fun contains(key: String, params: TagExistsParams): Boolean {
    log.debug("Attempting to test if {} contains the tag '{}'", handle, key)
    try {
      val res = getAllRaw(params)

      val out = key in res
      params.callback?.invoke(out)

      return out
    } catch (e: Throwable) {
      throw e.throwCorrect { "Failed to test for tag existence for $handle" }
    }
  }

  override fun count(params: TagCountParams): Int {
    log.debug("Attempting to count tags for {}", handle)
    try {
      val res = getAllRaw(params)

      val out = res.size
      params.callback?.invoke(out)

      return out
    } catch (e: Throwable) {
      throw e.throwCorrect { "Failed to count tags for $handle" }
    }
  }

  override fun delete(params: TagDeleteParams) {
    log.debug("Attempting to delete target tag from {}", handle)
    ObjectTagDeleter(handle, client, params).execute()
  }

  override fun deleteAll(params: DeleteAllTagsParams) {
    log.debug("Attempting to delete all tags from {}", handle)
    try {
      client.deleteObjectTags(DeleteObjectTagsArgs.builder()
        .bucket(handle.bucket)
        .region(params, handle)
        .`object`(handle.path)
        .headers(params.headers)
        .queryParams(params.queryParams)
        // TODO: Version ID
        .build())
    } catch (e: Throwable) {
      throw e.throwCorrect { "Failed to delete all tags from $handle" }
    }
  }

  override fun get(key: String): String? {
    log.debug("Attempting to get tag '{}' from {}", key, handle)
    try {
      val res = client.getObjectTags(GetObjectTagsArgs.builder()
        .bucket(handle.bucket)
        .region(handle.region)
        .`object`(handle.path)
        .build()).get()

      return res[key]
    } catch (e: Throwable) {
      throw e.throwCorrect {
        "Failed to get object tag '$key' for object '${handle.path}' in bucket '${handle.bucket.name}'"
      }
    }
  }

  override fun get(params: TagGetParams): TagMap {
    log.debug("Attempting to get target tags from {}", params)

    // If they didn't target any specific tags, then get all of them.
    if (params.tags.isEmpty) {
      val out = getAll()
      params.callback?.invoke(out)
      return out
    }

    try {
      val res = getAllRaw(params)

      // Filter the tags down to only those requested
      val tmp = HashMap<String, String>(params.tags.size)
      params.tags.forEach { if (it in res) tmp[it] = res[it]!! }

      // WOOO double map copy!
      val out = BasicTagMap(tmp)
      params.callback?.invoke(out)

      return out
    } catch (e: Throwable) {
      throw e.throwCorrect { "Failed to get tags for $handle" }
    }
  }

  override fun getAll(): TagMap {
    log.debug("Attempting to get all tags from {}", handle)
    try {
      val res = client.getObjectTags(GetObjectTagsArgs.builder()
        .bucket(handle.bucket)
        .region(handle.region)
        .`object`(handle.path)
        .build()).get()

      return BasicTagMap(res)
    } catch (e: Throwable) {
      throw e.throwCorrect { "Failed to get all tags for $handle" }
    }
  }

  override fun put(params: TagPutParams) {
    log.debug("Attempting to put tags onto {}", handle)
    if (params.tags.isEmpty) {
      log.debug("No tags specified for put")
      params.callback?.invoke()
      return
    }

    try {
      client.setObjectTags(SetObjectTagsArgs.builder()
        .bucket(handle.bucket)
        .region(params, handle)
        .`object`(handle.path)
        .tags(params.tags.toMap())
        .headers(params.headers)
        .queryParams(params.queryParams)
        // TODO: Version ID
        .build())

      params.callback?.invoke()
    } catch (e: Throwable) {
      throw e.throwCorrect { "Failed to put tags onto $handle" }
    }
  }

  @Suppress("NOTHING_TO_INLINE")
  private inline fun getAllRaw(params: RegionRequestParams): Map<String, String> {
    return client.getObjectTags(GetObjectTagsArgs.builder()
      .bucket(handle.bucket)
      .region(params, handle)
      .`object`(handle.path)
      .headers(params.headers)
      .queryParams(params.queryParams)
      // TODO: Version ID
      .build()).get()
  }
}