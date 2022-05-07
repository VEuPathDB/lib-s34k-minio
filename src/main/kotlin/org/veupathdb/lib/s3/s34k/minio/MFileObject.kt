package org.veupathdb.lib.s3.s34k.minio

import io.minio.MinioClient
import org.veupathdb.lib.s3.s34k.buckets.S3Bucket
import org.veupathdb.lib.s3.s34k.fields.Headers
import org.veupathdb.lib.s3.s34k.objects.FileObject
import java.io.File

internal class MFileObject(
  path: String,
  region: String?,
  headers: Headers,
  bucket: S3Bucket,
  client: MinioClient,
  override val localFile: File,
) : FileObject, MObject(path, region, headers, bucket, client)