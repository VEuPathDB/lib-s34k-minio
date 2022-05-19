package org.veupathdb.lib.s3.s34k.minio.operations

import io.minio.MinioClient
import io.minio.PutObjectArgs
import io.minio.StatObjectArgs
import org.veupathdb.lib.s3.s34k.buckets.S3Bucket
import org.veupathdb.lib.s3.s34k.minio.MObject
import org.veupathdb.lib.s3.s34k.minio.fields.MHeaders
import org.veupathdb.lib.s3.s34k.minio.util.*
import org.veupathdb.lib.s3.s34k.objects.S3Object
import org.veupathdb.lib.s3.s34k.params.`object`.touch.ObjectTouchParams

internal class ObjectToucher(
  private val bucket: S3Bucket,
  private val path:   String,
  private val params: ObjectTouchParams,
  private val minio:  MinioClient,
) {
  fun execute(): S3Object {

    val out: S3Object

    if (params.overwrite) {
      out = put()
    } else {
      out = get() ?: put()
    }

    params.callback?.invoke(out)

    return out
  }

  private fun get(): S3Object? {
    try {
      val res = minio.statObject(StatObjectArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        .`object`(path)
        .headers(params.headers, params.getParams.headers)
        .queryParams(params.queryParams, params.getParams.queryParams)
        .build())

      params.getParams.callback?.invoke()

      return MObject(
        res.`object`(),
        res.lastModified().toOffsetDateTime(),
        res.etag(),
        res.region(),
        MHeaders(res.headers()),
        bucket,
        minio
      )
    } catch (e: Throwable) {
      if (e.isNoSuchKey()) {
        params.getParams.callback?.invoke()
        return null
      }

      e.throwCorrect { "Failed ot get object '$path' from $bucket" }
    }
  }

  private fun put(): S3Object {
    try {
      val res = minio.putObject(PutObjectArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        .`object`(path)
        .stream(ByteArray(0).inputStream(), 0, 0)
        .optContentType(params.contentType)
        .headers(params.headers, params.putParams.headers)
        .queryParams(params.queryParams, params.putParams.queryParams)
        .build())
      params.putParams.callback?.invoke()

      return MObject(
        res.`object`(),
        null,
        res.etag(),
        res.region(),
        MHeaders(res.headers()),
        bucket,
        minio,
      )
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to put object '$path' into '$bucket'" }
    }
  }
}