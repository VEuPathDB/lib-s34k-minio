@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import io.minio.*
import org.veupathdb.lib.s3.s34k.S3Bucket
import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.errors.InvalidRequestConfigException
import org.veupathdb.lib.s3.s34k.params.AbstractRequestParams
import org.veupathdb.lib.s3.s34k.params.bucket.BucketExistsParams
import org.veupathdb.lib.s3.s34k.params.bucket.BucketGetParams
import org.veupathdb.lib.s3.s34k.params.bucket.BucketListParams
import org.veupathdb.lib.s3.s34k.params.bucket.BucketTagGetParams
import org.veupathdb.lib.s3.s34k.params.`object`.*

// region: Builder Assists

internal inline fun <R: BaseArgs.Builder<R, B>, B: BaseArgs> AbstractRequestParams.addHTTPExtras(
  builder: BaseArgs.Builder<in R, out B>
) {
  headers.ifNotEmpty { builder.extraHeaders(it.toMultiMap()) }
  queryParams.ifNotEmpty { builder.extraQueryParams(it.toMultiMap()) }
}


internal inline fun <R: BucketArgs.Builder<R, B>, B: BucketArgs> AbstractRequestParams.addBucketParams(
  bucket: String,
  region: String?,
  builder: BucketArgs.Builder<in R, out BucketArgs>
) {
  builder.bucket(bucket)
  region.ifNotNull(builder::region)
  addHTTPExtras(builder)
}


internal inline fun <R: ObjectArgs.Builder<R, B>, B: ObjectArgs> SealedObjReqParams.addObjectParams(
  bucket: String,
  region: String?,
  builder: ObjectArgs.Builder<in R, out B>
) {
  builder.`object`(reqPath())
  addBucketParams(bucket, region, builder)
}

// endregion


// region: Bucket Params

internal inline fun BucketExistsParams.toMinio() =
  BucketExistsArgs.builder().also { addBucketParams(reqBucket(), region, it) }.build()


internal inline fun BucketListParams.toMinio() =
  ListBucketsArgs.builder().also { addHTTPExtras(it) }.build()


internal inline fun BucketGetParams.toMinio() =
  ListBucketsArgs.builder().also { addHTTPExtras(it) }.build()


context(S3Bucket)
internal inline fun BucketTagGetParams.toMinio() =
  GetBucketTagsArgs.builder().also { addBucketParams(name, region, it) }.build()

// endregion


// region: Object Params

context(S3Bucket)
internal inline fun ObjectDownloadParams.toMinio() =
  GetObjectArgs.builder().also { addObjectParams(name, region, it) }.build()


context(S3Bucket)
internal inline fun ObjectExistsParams.toMinio() =
  StatObjectArgs.builder().also { addObjectParams(name, region, it) }.build()


context(S3Bucket)
internal inline fun ObjectFilePutParams.toMinio() =
  UploadObjectArgs.builder().also {
    it.filename(localFile.reqLFExists(this).absolutePath, partSize)
    addObjectParams(name, region, it)
  }


context(S3Bucket)
internal inline fun ObjectGetParams.toMinio() =
  GetObjectArgs.builder().also { addObjectParams(name, region, it) }.build()


context(S3Bucket)
internal inline fun ObjectTagGetParams.toMinio() =
  GetObjectTagsArgs.builder().also { addObjectParams(name, region, it) }.build()


context(S3Bucket)
internal inline fun ObjectStatParams.toMinio(bucket: String, region: String) =
  StatObjectArgs.builder().also { addObjectParams(name, region, it) }.build()

// endregion
