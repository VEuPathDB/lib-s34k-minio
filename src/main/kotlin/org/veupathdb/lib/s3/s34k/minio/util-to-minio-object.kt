@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import io.minio.*
import org.veupathdb.lib.s3.s34k.fields.BucketName
import org.veupathdb.lib.s3.s34k.requests.`object`.*


// region Tag Params

internal inline fun S3ObjectTagGetParams.toMinio(bucket: BucketName, region: String?) =
  GetObjectTagsArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(headers.toMultiMap())
  }.build()

internal inline fun S3ObjectTagCreateParams.toMinio(bucket: BucketName, region: String?) =
  SetObjectTagsArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.tags(tags.toMap())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

// endregion Tag Params

// region Object Put Params

internal inline fun S3DirectoryCreateParams.toMinio(bucket: BucketName, region: String?) =
  PutObjectArgs.builder().also {
    var path = reqPath()

    if (!path.endsWith('/'))
      path = "$path/"

    it.`object`(path)
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.stream(ByteArray(0).inputStream(), 0, -1)
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
    it.tags(tags.toMap())
  }.build()

internal inline fun S3FileUploadParams.toMinio(bucket: BucketName, region: String?) =
  UploadObjectArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    contentType.ifSet(it::contentType)
    it.filename(localFile.reqLFExists(this).absolutePath, partSize.toLong())
    it.tags(tags.toMap())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun S3ObjectTouchParams.toMinio(bucket: BucketName, region: String?) =
  PutObjectArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.stream(ByteArray(0).inputStream(), 0, -1)
    it.tags(tags.toMap())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun S3StreamingObjectCreateParams.toMinio(bucket: BucketName, region: String?) =
  PutObjectArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    contentType.ifSet(it::contentType)
    it.stream(stream, -1, partSize.toLong())
    it.tags(tags.toMap())
    it.headers(headers.toMultiMap())
    it.extraQueryParams(headers.toMultiMap())
  }.build()

// endregion Object Put Params

// region Object Get params

internal inline fun S3ObjectDownloadParams.toMinio(bucket: BucketName, region: String?) =
  GetObjectArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun S3ObjectExistsParams.toMinio(bucket: BucketName, region: String?) =
  StatObjectArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun S3ObjectGetParams.toMinio(bucket: BucketName, region: String?) =
  GetObjectArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()


internal inline fun S3ObjectStatParams.toMinio(bucket: BucketName, region: String?) =
  StatObjectArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

// endregion Object Get Params

internal inline fun S3ObjectDeleteParams.toMinio(bucket: BucketName, region: String?) =
  RemoveObjectArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun S3MultiObjectDeleteParams.toMinio(bucket: BucketName, region: String?) =
  RemoveObjectsArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.objects(paths.toDelObjList())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun S3ObjectListParams.toMinio(bucket: BucketName, region: String?) =
  ListObjectsArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()