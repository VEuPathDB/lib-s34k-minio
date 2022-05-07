@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio.util

import io.minio.BaseArgs
import io.minio.BucketArgs
import io.minio.ListObjectsArgs
import io.minio.ObjectArgs
import io.minio.PutObjectArgs
import io.minio.UploadObjectArgs
import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.buckets.S3Bucket
import org.veupathdb.lib.s3.s34k.fields.Headers
import org.veupathdb.lib.s3.s34k.fields.QueryParams
import org.veupathdb.lib.s3.s34k.objects.S3Object
import org.veupathdb.lib.s3.s34k.params.RegionRequestParams

// region Base Args

internal inline fun <B: BaseArgs.Builder<B, A>, A: BaseArgs> B.headers(head: Headers): B =
  extraHeaders(head.toMultiMap())

internal inline fun <B: BaseArgs.Builder<B, A>, A: BaseArgs> B.headers(a: Headers, b: Headers): B =
  extraHeaders(multiMap(a, b))

internal inline fun <B: BaseArgs.Builder<B, A>, A: BaseArgs> B.queryParams(params: QueryParams): B =
  extraQueryParams(params.toMultiMap())

internal inline fun <B: BaseArgs.Builder<B, A>, A: BaseArgs> B.queryParams(a: QueryParams, b: QueryParams): B =
  extraQueryParams(multiMap(a, b))

// region Base Args

// region Bucket Args

internal inline fun <B: BucketArgs.Builder<B, A>, A: BucketArgs> B.bucket(bucket: S3Bucket): B =
  bucket(bucket.name.name)

internal inline fun <B: BucketArgs.Builder<B, A>, A: BucketArgs> B.region(bucket: S3Bucket): B =
  region(bucket.region)

internal inline fun <B: BucketArgs.Builder<B, A>, A: BucketArgs> B.region(params: RegionRequestParams, bucket: S3Bucket): B =
  region(params.region ?: bucket.region)

internal inline fun <B: BucketArgs.Builder<B, A>, A: BucketArgs> B.region(params: RegionRequestParams, client: S3Client): B =
  region(params.region ?: client.defaultRegion)


// endregion Bucket Args

// region Object Args

internal inline fun <B: ObjectArgs.Builder<B, A>, A: ObjectArgs> B.region(params: RegionRequestParams, handle: S3Object): B =
  region(params.region ?: handle.region)

// endregion Object Args

internal inline fun ListObjectsArgs.Builder.optPrefix(prefix: String?) = also { prefix?.let(this::prefix) }


internal inline fun PutObjectArgs.Builder.optContentType(ct: String?) = also { ct?.let(this::contentType) }

internal inline fun UploadObjectArgs.Builder.optContentType(ct: String?) = also { ct?.let(this::contentType) }