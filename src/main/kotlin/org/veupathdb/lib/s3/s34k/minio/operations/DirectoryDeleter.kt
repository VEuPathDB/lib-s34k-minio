package org.veupathdb.lib.s3.s34k.minio.operations

import io.minio.ListObjectsArgs
import io.minio.MinioClient
import io.minio.RemoveObjectsArgs
import io.minio.Result
import io.minio.messages.DeleteObject
import io.minio.messages.Item
import org.veupathdb.lib.s3.s34k.S3ErrorCode
import org.veupathdb.lib.s3.s34k.buckets.S3Bucket
import org.veupathdb.lib.s3.s34k.errors.MultiObjectDeleteError
import org.veupathdb.lib.s3.s34k.errors.ObjectDeleteError
import org.veupathdb.lib.s3.s34k.errors.S34KError
import org.veupathdb.lib.s3.s34k.minio.util.*
import org.veupathdb.lib.s3.s34k.params.`object`.directory.*
import java.util.stream.Stream

internal class DirectoryDeleter(
  private val bucket: S3Bucket,
  private val prefix: String,
  private val params: DirectoryDeleteParams,
  private val minio: MinioClient
) {
  fun execute() {
    deleteObjects(listObjects())
    params.callback?.invoke()
  }

  private fun listObjects(): Stream<String> {
    try {
      val res = minio.listObjects(ListObjectsArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        .recursive(true)
        .maxKeys(params.listParams.pageSize.toInt())
        .prefix(prefix.addSlash())
        .headers(params.headers, params.listParams.headers)
        .queryParams(params.queryParams, params.listParams.queryParams)
        .build())
        .toStream()
        .map(Result<Item>::get)
        .map(Item::objectName)

      params.listParams.callback?.invoke()

      return res
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to list objects in bucket '$bucket'" }
    }
  }

  private fun deleteObjects(items: Stream<String>) {
    try {
      val res = minio.removeObjects(RemoveObjectsArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        .bypassGovernanceMode(params.deleteParams.bypassGovernance)
        .objects(items.map(::DeleteObject).toIterable())
        .headers(params.headers, params.deleteParams.headers)
        .queryParams(params.queryParams, params.deleteParams.queryParams)
        .build())

      val writ = res.iterator()

      // If the server returned 1 or more object delete errors, go through them
      // to see if we care about them.
      if (writ.hasNext()) {
        val errs = ArrayList<ObjectDeleteError>(10)

        // Iterate over all the returned errors.
        for (err in writ) {
          val unwrapped = err.get()

          // If the error is NoSuchKey, then we can ignore it because we were
          // trying to delete the object anyway.
          if (unwrapped.code() != S3ErrorCode.NoSuchKey)
            errs.add(ObjectDeleteError(unwrapped.objectName(), unwrapped.message(), unwrapped.code()))
        }

        // If we had any errors that we cared about in the error response, then
        // throw here.
        if (errs.isNotEmpty())
          throw MultiObjectDeleteError(bucket.name, errs)
      }

      params.deleteParams.callback?.invoke()
    } catch (e: Throwable) {
      if (e is S34KError)
        throw e
      e.throwCorrect { "Failed to delete objects from $bucket" }
    }
  }
}