package org.veupathdb.lib.s3.s34k.minio

import org.veupathdb.lib.s3.s34k.S3Bucket
import org.veupathdb.lib.s3.s34k.S3Headers
import org.veupathdb.lib.s3.s34k.S3StreamObject
import java.io.InputStream

class S3StreamObjectImpl(
  bucket: S3Bucket,
  region: String,
  headers: S3Headers,
  path: String,

  override val stream: InputStream
) : S3StreamObject, S3ObjectImpl(bucket, region, headers, path)