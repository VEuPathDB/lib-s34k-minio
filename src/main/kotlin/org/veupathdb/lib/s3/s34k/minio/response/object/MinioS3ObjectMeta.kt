package org.veupathdb.lib.s3.s34k.minio.response.`object`

import io.minio.StatObjectResponse
import org.veupathdb.lib.s3.s34k.S3Bucket
import org.veupathdb.lib.s3.s34k.core.response.`object`.BasicS3ObjectMeta
import org.veupathdb.lib.s3.s34k.minio.fields.headers.MinioS3Headers
import org.veupathdb.lib.s3.s34k.minio.fromMinio

class MinioS3ObjectMeta(bucket: S3Bucket, res: StatObjectResponse) :
  BasicS3ObjectMeta(
    bucket,
    MinioS3Headers(res.headers()),
    res.region() ?: bucket.defaultRegion,
    res.`object`(),
    res.contentType(),
    res.etag(),
    res.lastModified().toOffsetDateTime(),
    res.legalHold().fromMinio(),
    res.retentionMode().fromMinio(),
    res.size()
  )