package org.veupathdb.lib.s3.s34k.minio.operations

import io.minio.ListObjectsArgs
import io.minio.MinioClient
import io.minio.RemoveObjectArgs
import io.minio.RemoveObjectsArgs
import io.minio.Result
import io.minio.messages.DeleteError
import io.minio.messages.DeleteObject
import io.minio.messages.Item
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.minio.*
import org.veupathdb.lib.s3.s34k.requests.`object`.ObjectDeleteError
import org.veupathdb.lib.s3.s34k.requests.`object`.directory.DirectoryNotEmptyError
import org.veupathdb.lib.s3.s34k.requests.`object`.directory.DirectoryObjectDeleteError
import org.veupathdb.lib.s3.s34k.requests.`object`.directory.S3DirectoryDeleteParams
import org.veupathdb.lib.s3.s34k.response.bucket.S3Bucket
import java.util.stream.Collectors
import java.util.stream.Stream

class DirectoryDeleter(
  private val minio:  MinioClient,
  private val bucket: S3Bucket,
  private val params: S3DirectoryDeleteParams,
) {

  private val Log = LoggerFactory.getLogger(this::class.java)

  // TODO: this should be configurable in the params.
  private val maxKeys = 1000

  fun execute(): Boolean {
    Log.trace("execute()")

    return if (params.recursive) {
      Log.debug("Recursively deleting directory '{}' from bucket '{}'", params.path, bucket.bucketName)
      deleteRecursive()
    } else {
      Log.debug("Non-recursively deleting directory '{}' from bucket '{}'", params.path, bucket.bucketName)
      deleteSimple()
    }
  }

  fun deleteSimple(): Boolean {
    Log.trace("deleteSimple()")

    // If there exists even one object with the target path as a prefix, then
    // the 'directory' is not empty and cannot be deleted.
    if (fetchSubKeys().findAny().isPresent)
      throw DirectoryNotEmptyError(bucket.bucketName.name, params.path!!)

    if (!objectExists()) {
      return params.callback.invoke("deleteSimple", Log, false)
    }

    try {
      minio.removeObject(RemoveObjectArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        .`object`(params.reqPath())
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())
      return params.callback.invoke("deleteSimple", Log, true)
    } catch (e: Throwable) {
      throw e.toCorrect {
        "Failed to delete directory '${params.path}'"
      }
    }
  }

  fun deleteRecursive(): Boolean {
    Log.trace("deleteRecursive()")

    // We need to consume the stream here to get a count of the files to delete.
    // If this number is 0, then we can skip the bulk delete and return false
    val deleteables = fetchSubKeys()
      .map(::DeleteObject)
      .collect(Collectors.toList())

    if (deleteables.isNotEmpty()) {
      // Attempt to delete the files found
      Log.debug("Attempting to delete all files in directory '{}' from bucket '{}'", params.path, bucket.bucketName)
      try {
        val res = minio.removeObjects(RemoveObjectsArgs.builder()
          .bucket(bucket)
          .region(params, bucket)
          .objects(deleteables)
          .headers(params.headers)
          .queryParams(params.queryParams)
          .build())

        // Collect delete errors from the bulk delete to report if any occurred.
        val failed = ArrayList<ObjectDeleteError>(deleteables.size)

        // Stream over the delete results from the bulk object delete
        res.toStream()
          // Pop the actual error out of the result
          .map(Result<DeleteError>::get)
          // Convert the minio error into an S34K error
          .map { ObjectDeleteError(it.objectName(), it.message(), it.code()) }
          // Add each to the failed list
          .forEach(failed::add)

        // If we did have an error during our bulk delete, stop here and report it
        if (failed.isNotEmpty())
          throw DirectoryObjectDeleteError(bucket.bucketName.name, params.path!!, failed)
      } catch (e: Throwable) {
        throw e.toCorrect { "Failed to delete files in directory '${params.path}' from bucket '${bucket.bucketName}'" }
      }
    }

    // Does directory entry itself exist?
    val exists = objectExists()

    // If not, then we can bail here because we have nothing to delete.
    if (!exists)
      // Return whether we deleted anything as an indicator of whether the
      // 'directory' existed.  If there were objects to delete, then they were
      // subkeys of the target which means the target key was present.
      return deleteables.isNotEmpty()

    // Attempt to delete the directory itself
    try {
      // Remove the directory key itself.
      minio.removeObject(RemoveObjectArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        .`object`(params.reqPath())
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())
    } catch (e: Throwable) {
      throw e.toCorrect { "Failed to delete directory '${params.path}' from bucket '${bucket.bucketName}'" }
    }

    return true
  }

  fun objectExists(): Boolean {
    Log.trace("objectExists()")

    return bucket.objectExists {
      path = params.reqPath()
      region = params.region
      // TODO: multistage headers
      // TODO: multistage query params
      // TODO: multistage callback
    }
  }

  fun fetchSubKeys(): Stream<String> {
    Log.trace("fetchSubKeys()")

    val path = params.reqPath().asPath()

    Log.debug("Retrieving list of objects with the prefix '{}'", path)
    try {
      // TODO: multistage callback
      return minio.listObjects(ListObjectsArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        .recursive(true)
        .prefix(path)
        .maxKeys(maxKeys)
        // TODO: multistage headers
        // TODO: multistage query params
        .build())
        .toStream()
        .map(Result<Item>::get)
        .map(Item::objectName)
    } catch (e: Throwable) {
      throw e.toCorrect {
        "Failed to fetch object list with prefix '$path'"
      }
    }
  }

}