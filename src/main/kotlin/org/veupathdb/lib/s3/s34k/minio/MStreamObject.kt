package org.veupathdb.lib.s3.s34k.minio

import io.minio.MinioClient
import org.veupathdb.lib.s3.s34k.buckets.S3Bucket
import org.veupathdb.lib.s3.s34k.fields.Headers
import org.veupathdb.lib.s3.s34k.objects.StreamObject
import java.io.InputStream
import java.time.OffsetDateTime

internal class MStreamObject(
  path: String,
  lastModified: OffsetDateTime?,
  eTag: String?,
  size: Long,
  region: String?,
  override val stream: InputStream,
  headers: Headers,
  bucket: S3Bucket,
  client: MinioClient
) : StreamObject, MObject(path, lastModified, eTag, size, region ,headers, bucket, client) {
  override fun close() = stream.close()
}