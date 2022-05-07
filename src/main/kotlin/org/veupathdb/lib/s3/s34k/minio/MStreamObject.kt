package org.veupathdb.lib.s3.s34k.minio

import io.minio.MinioClient
import org.veupathdb.lib.s3.s34k.Bucket
import org.veupathdb.lib.s3.s34k.StreamObject
import org.veupathdb.lib.s3.s34k.fields.Headers
import java.io.InputStream

internal class MStreamObject(
  path: String,
  region: String?,
  override val stream: InputStream,
  headers: Headers,
  bucket: Bucket,
  client: MinioClient
) : StreamObject, MObject(path, region ,headers, bucket, client) {
  override fun close() = stream.close()
}