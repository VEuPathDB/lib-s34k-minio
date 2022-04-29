package org.veupathdb.lib.s3.s34k.minio.response.`object`

import io.minio.messages.Item
import io.minio.Result
import org.veupathdb.lib.s3.s34k.S3Bucket
import org.veupathdb.lib.s3.s34k.core.response.`object`.BasicS3ObjectList
import org.veupathdb.lib.s3.s34k.minio.toS3Object
import org.veupathdb.lib.s3.s34k.response.`object`.S3Object

class MinioS3ObjectList(
  bucket: S3Bucket,
  values: Iterable<Result<Item>>
) : BasicS3ObjectList(parseValues(bucket, values)) {

  private companion object {
    @JvmStatic
    fun parseValues(bucket: S3Bucket, values: Iterable<Result<Item>>): Map<String, S3Object> {
      val out = HashMap<String, S3Object>(32)

      values.forEach { with(it.get()) { out[objectName()] = toS3Object(bucket) } }

      return out
    }
  }
}