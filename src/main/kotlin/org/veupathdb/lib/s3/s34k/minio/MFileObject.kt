package org.veupathdb.lib.s3.s34k.minio

import io.minio.MinioClient
import org.veupathdb.lib.s3.s34k.buckets.S3Bucket
import org.veupathdb.lib.s3.s34k.fields.Headers
import org.veupathdb.lib.s3.s34k.objects.FileObject
import java.io.File
import java.time.OffsetDateTime

internal class MFileObject(
  path:         String,
  lastModified: OffsetDateTime?,
  eTag:         String,
  size:         Long,
  region:       String?,
  headers:      Headers,
  bucket:       S3Bucket,
  client:       MinioClient,
  override val localFile: File,
) : FileObject, MObject(path, lastModified, eTag, size, region, headers, bucket, client)