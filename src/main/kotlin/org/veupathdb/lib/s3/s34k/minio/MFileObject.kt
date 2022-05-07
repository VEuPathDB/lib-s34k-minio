package org.veupathdb.lib.s3.s34k.minio

import io.minio.MinioClient
import org.veupathdb.lib.s3.s34k.Bucket
import org.veupathdb.lib.s3.s34k.FileObject
import org.veupathdb.lib.s3.s34k.fields.Headers
import java.io.File

internal class MFileObject(
  path: String,
  region: String?,
  headers: Headers,
  bucket: Bucket,
  client: MinioClient,
  override val localFile: File,
) : FileObject, MObject(path, region, headers, bucket, client)