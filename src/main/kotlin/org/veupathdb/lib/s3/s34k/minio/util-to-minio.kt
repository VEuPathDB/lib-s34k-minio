@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import io.minio.*
import org.veupathdb.lib.s3.s34k.params.RequestParams
import org.veupathdb.lib.s3.s34k.params.bucket.*
import org.veupathdb.lib.s3.s34k.params.`object`.*

// region Builder Assists

/**
 * Appends the configured headers and query parameters to the target builder.
 *
 * @param R Builder type
 *
 * @param B Args container type.
 *
 * @param builder Builder instance.
 */
private inline fun <R: BaseArgs.Builder<R, B>, B: BaseArgs> RequestParams.addHTTPExtras(
  builder: BaseArgs.Builder<in R, out B>
) {
  headers.ifNotEmpty { builder.extraHeaders(it.toMultiMap()) }
  queryParams.ifNotEmpty { builder.extraQueryParams(it.toMultiMap()) }
}


/**
 * Requires the bucket parameter is set, appends it, appends the region if set,
 * then appends the configured headers and query parameters.
 *
 * @param R Builder type.
 *
 * @param B Args container type.
 *
 * @param bucket Name of the target bucket.
 *
 * @param region Optional region value for the request.
 *
 * @param builder Builder instance.
 */
private inline fun <R: BucketArgs.Builder<R, B>, B: BucketArgs> RequestParams.addBucketParams(
  bucket: BucketName?,
  region: String?,
  builder: BucketArgs.Builder<R, out BucketArgs>
) {
  builder.bucket(reqSet("bucket", bucket).name)
  region.ifNotNull(builder::region)
  addHTTPExtras(builder)
}


/**
 * Requires the bucket and object parameters to be set, appends them, appends
 * the region if set, then appends the configured headers and query parameters.
 *
 * @param R Builder type.
 *
 * @param B Args container type.
 *
 * @param bucket Name of the target bucket.
 *
 * @param region Optional region value.
 *
 * @param builder Builder instance.
 */
private inline fun <R: ObjectArgs.Builder<R, B>, B: ObjectArgs> SealedObjReqParams.addObjectParams(
  bucket: BucketName?,
  region: String?,
  builder: ObjectArgs.Builder<in R, out B>
) {
  builder.`object`(reqPath())
  addBucketParams(bucket, region, builder)
}

// endregion

// region Bucket Params

internal inline fun BucketExistsParams.toMinio() =
  BucketExistsArgs.builder().also { addBucketParams(bucket, region, it) }.build()


internal inline fun BucketListParams.toMinio() =
  ListBucketsArgs.builder().also { addHTTPExtras(it) }.build()


internal inline fun BucketGetParams.toMinio() =
  ListBucketsArgs.builder().also { addHTTPExtras(it) }.build()


internal inline fun BucketPutParams.toMinio() =
  MakeBucketArgs.builder().also { addBucketParams(bucket, region, it) }.build()


internal inline fun BucketTagGetParams.toMinio(name: BucketName, region: String?) =
  GetBucketTagsArgs.builder().also { addBucketParams(name, region, it) }.build()


internal inline fun BucketTagPutParams.toMinio(name: BucketName, region: String?) =
  SetBucketTagsArgs.builder().also {
    addBucketParams(name, region, it)
    it.tags(getTagsMap())
  }.build()

// endregion


// region Object Params

internal inline fun DirectoryPutParams.toMinio(bucket: BucketName, region: String?) =
  PutObjectArgs.builder().also {
    var path = reqPath()
    if (!path.endsWith('/'))
      path = "$path/"

    it.`object`(path)
    it.bucket(bucket.name)
    region.ifSet(it::region)
    it.stream(ByteArray(0).inputStream(), 0, -1)
    it.extraHeaders(headers.toMultiMap())
    it.extraQueryParams(queryParams.toMultiMap())
    it.tags(getTagsMap())
  }.build()

internal inline fun ObjectDownloadParams.toMinio(bucket: BucketName, region: String?) =
  GetObjectArgs.builder().also { addObjectParams(bucket, region, it) }.build()


internal inline fun ObjectExistsParams.toMinio(bucket: BucketName, region: String?) =
  StatObjectArgs.builder().also { addObjectParams(bucket, region, it) }.build()


internal inline fun ObjectFilePutParams.toMinio(bucket: BucketName, region: String?) =
  UploadObjectArgs.builder().also {
    it.filename(localFile.reqLFExists(this).absolutePath, partSize)
    it.tags(getTagsMap())
    addObjectParams(bucket, region, it)
  }.build()


internal inline fun ObjectTouchParams.toMinio(bucket: BucketName, region: String?) =
  PutObjectArgs.builder().also {
    addBucketParams(bucket, region, it)
    it.stream(ByteArray(0).inputStream(), 0, -1)
    it.tags(getTagsMap())
  }.build()


internal inline fun ObjectGetParams.toMinio(bucket: BucketName, region: String?) =
  GetObjectArgs.builder().also { addObjectParams(bucket, region, it) }.build()


internal inline fun ObjectPutParams.toMinio(bucket: BucketName, region: String?) =
  PutObjectArgs.builder().also {
    addBucketParams(bucket, region, it)
    it.stream(stream, -1, partSize)
    it.tags(getTagsMap())
  }.build()


internal inline fun ObjectTagGetParams.toMinio(bucket: BucketName, region: String?) =
  GetObjectTagsArgs.builder().also { addObjectParams(bucket, region, it) }.build()

internal inline fun ObjectTagPutParams.toMinio(bucket: BucketName, region: String?) =
  SetObjectTagsArgs.builder().also {
    addObjectParams(bucket, region, it)
    it.tags(getTagsMap())
  }.build()


internal inline fun ObjectStatParams.toMinio(bucket: BucketName, region: String?) =
  StatObjectArgs.builder().also { addObjectParams(bucket, region, it) }.build()

// endregion
