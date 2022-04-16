package org.veupathdb.lib.s3.s34k.minio

import io.minio.StatObjectResponse
import org.veupathdb.lib.s3.s34k.*
import java.time.OffsetDateTime

class S3ObjectMetaImpl(
               bucket:        S3Bucket,
               region:        String?,
               headers:       S3Headers,
               path:          String,
  override val contentType:   String,
  override val eTag:          String,
  override val lastModified:  OffsetDateTime,
  override val legalHold:     S3LegalHold?,
  override val retentionMode: S3RetentionMode?,
  override val size: Long,
) : S3ObjectMeta, S3ObjectResponseImpl(bucket, region, headers, path) {

  constructor(bucket: S3Bucket, region: String?, res: StatObjectResponse) : this (
    bucket,
    region,
    S3HeadersImpl(res.headers()),
    res.`object`(),
    res.contentType(),
    res.etag(),
    res.lastModified().toOffsetDateTime(),
    res.legalHold().fromMinio(),
    res.retentionMode().fromMinio(),
    res.size(),
  )
}