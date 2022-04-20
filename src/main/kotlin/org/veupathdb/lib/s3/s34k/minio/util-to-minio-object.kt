@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import io.minio.*
import org.veupathdb.lib.s3.s34k.params.bucket.BucketName
import org.veupathdb.lib.s3.s34k.params.`object`.*


// region Tag Params

internal inline fun ObjectTagDeleteParams.toObjectTagDelete(bucket: BucketName, region: String?) =
  DeleteObjectTagsArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun ObjectTagDeleteParams.toObjectTagFetch(bucket: BucketName, region: String?) =
  GetObjectTagsArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun ObjectTagDeleteParams.toObjectTagPut(
  bucket: BucketName,
  region: String?,
  tags: Map<String, String>
) =
  SetObjectTagsArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.tags(tags)
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun ObjectTagGetParams.toMinio(bucket: BucketName, region: String?) =
  GetObjectTagsArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(headers.toMultiMap())
  }.build()

internal inline fun ObjectTagPutParams.toMinio(bucket: BucketName, region: String?) =
  SetObjectTagsArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.tags(getTagsMap())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

// endregion Tag Params

// region Object Put Params

internal inline fun DirectoryPutParams.toMinio(bucket: BucketName, region: String?) =
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
    it.tags(getTagsMap())
  }.build()

internal inline fun ObjectFilePutParams.toMinio(bucket: BucketName, region: String?) =
  UploadObjectArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    contentType.ifSet(it::contentType)
    it.filename(localFile.reqLFExists(this).absolutePath, partSize)
    it.tags(getTagsMap())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun ObjectTouchParams.toMinio(bucket: BucketName, region: String?) =
  PutObjectArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    contentType.ifSet(it::contentType)
    it.stream(ByteArray(0).inputStream(), 0, -1)
    it.tags(getTagsMap())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun ObjectPutParams.toMinio(bucket: BucketName, region: String?) =
  PutObjectArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    contentType.ifSet(it::contentType)
    it.stream(stream, -1, partSize)
    it.tags(getTagsMap())
    it.headers(headers.toMultiMap())
    it.extraQueryParams(headers.toMultiMap())
  }.build()

// endregion Object Put Params

// region Object Get params

internal inline fun ObjectDownloadParams.toMinio(bucket: BucketName, region: String?) =
  GetObjectArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun ObjectExistsParams.toMinio(bucket: BucketName, region: String?) =
  StatObjectArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

internal inline fun ObjectGetParams.toMinio(bucket: BucketName, region: String?) =
  GetObjectArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()


internal inline fun ObjectStatParams.toMinio(bucket: BucketName, region: String?) =
  StatObjectArgs.builder().also {
    it.bucket(bucket.name)
    it.region(this.region ?: region)
    it.`object`(reqPath())
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
  }.build()

// region Object Get Params


internal inline fun String.toObjectTagDelete(bucket: BucketName, region: String?) =
  DeleteObjectTagsArgs.builder().also {
    it.bucket(bucket.name)
    it.region(region)
    it.`object`(this)
  }.build()

internal inline fun String.toObjectTagFetch(bucket: BucketName, region: String?) =
  GetObjectTagsArgs.builder().also {
    it.bucket(bucket.name)
    it.region(region)
    it.`object`(this)
  }.build()