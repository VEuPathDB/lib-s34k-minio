package org.veupathdb.lib.s3.s34k.minio.operations

import io.minio.MinioClient
import io.minio.RemoveObjectArgs
import io.minio.StatObjectArgs
import org.veupathdb.lib.s3.s34k.Bucket
import org.veupathdb.lib.s3.s34k.ObjectMeta
import org.veupathdb.lib.s3.s34k.core.BasicObjectMeta
import org.veupathdb.lib.s3.s34k.minio.fields.MHeaders
import org.veupathdb.lib.s3.s34k.minio.util.*
import org.veupathdb.lib.s3.s34k.params.DeleteParams
import org.veupathdb.lib.s3.s34k.params.`object`.ObjectExistsParams
import org.veupathdb.lib.s3.s34k.params.`object`.ObjectStatParams


internal fun ObjectDelete(bucket: Bucket, path: String, params: DeleteParams, minio: MinioClient) {
  try {
    minio.removeObject(RemoveObjectArgs.builder()
      .bucket(bucket)
      .region(params, bucket)
      .`object`(path)
      // TODO: governance mode
      .headers(params.headers)
      .queryParams(params.queryParams)
      // TODO: version ID
      .build())
  } catch (e: Throwable) {
    if (!e.isNoSuchKey())
      e.throwCorrect { "Failed to delete object '$path' from $bucket" }
  }

  params.callback?.invoke()
}


internal fun ObjectExists(bucket: Bucket, path: String, params: ObjectExistsParams, minio: MinioClient) : Boolean {
  try {
    minio.statObject(StatObjectArgs.builder()
      .bucket(bucket)
      .region(params, bucket)
      .`object`(path)
      .headers(params.headers)
      .queryParams(params.queryParams)
      .build())

    params.callback?.invoke(true)
    return true
  } catch (e: Throwable) {
    if (e.isNoSuchKey()) {
      params.callback?.invoke(false)
      return false
    }

    e.throwCorrect { "Failed to test if object '$path' exists in $bucket" }
  }
}


internal fun StatObject(
  bucket: Bucket,
  path:   String,
  params: ObjectStatParams,
  minio:  MinioClient,
) : ObjectMeta? {
  try {
    val res = minio.statObject(StatObjectArgs.builder()
      .bucket(bucket)
      .region(params, bucket)
      .`object`(path)
      .headers(params.headers)
      .queryParams(params.queryParams)
      .build())

    val out = BasicObjectMeta(
      bucket,
      res.`object`(),
      res.region(),
      res.contentType(),
      res.size(),
      res.lastModified().toOffsetDateTime(),
      res.etag(),
      MHeaders(res.headers()),
      res.legalHold().toS34K(),
      res.retentionMode().toS34K()
    )

    params.callback?.invoke(out)

    return out
  } catch (e: Throwable) {
    if (e.isNoSuchKey()) {
      params.callback?.invoke(null)
      return null
    }

    e.throwCorrect { "Failed to stat object $path in $bucket" }
  }
}
