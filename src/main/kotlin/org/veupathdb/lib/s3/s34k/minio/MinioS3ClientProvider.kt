package org.veupathdb.lib.s3.s34k.minio

import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.S3ClientProvider
import org.veupathdb.lib.s3.s34k.S3Config

/**
 * MinIO S3 Client Implementation Provider
 */
class MinioS3ClientProvider : S3ClientProvider {

  override fun new(config: S3Config): S3Client = MinioS3Client(config)
}