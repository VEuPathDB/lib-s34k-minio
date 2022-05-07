package org.veupathdb.lib.s3.s34k.minio.operations

import io.minio.MinioClient
import io.minio.RemoveBucketArgs
import org.veupathdb.lib.s3.s34k.BucketName
import org.veupathdb.lib.s3.s34k.minio.util.headers
import org.veupathdb.lib.s3.s34k.minio.util.isNoSuchBucket
import org.veupathdb.lib.s3.s34k.minio.util.queryParams
import org.veupathdb.lib.s3.s34k.minio.util.throwCorrect
import org.veupathdb.lib.s3.s34k.params.bucket.BucketDeleteParams

internal fun BucketDelete(
  name:   BucketName,
  region: String?,
  params: BucketDeleteParams,
  minio:  MinioClient,
) {
  try {
    minio.removeBucket(RemoveBucketArgs.builder()
      .bucket(name.name)
      .region(params.region ?: region)
      .headers(params.headers)
      .queryParams(params.queryParams)
      .build())
  } catch (e: Throwable) {
    if (!e.isNoSuchBucket())
      e.throwCorrect { "Failed to remove bucket '$name'" }
  }

  params.callback?.invoke()
}