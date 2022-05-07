package org.veupathdb.lib.s3.s34k.minio

import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.S3Config
import org.veupathdb.lib.s3.s34k.minio.util.makeUrl

internal class MClient(config: S3Config) : S3Client {

  private val client = io.minio.MinioClient.builder()
    .region(config.region)
    .endpoint(config.makeUrl())
    .credentials(config.accessKey, config.secretKey)
    .build()

  override val defaultRegion = config.region

  override val buckets = BucketContainer(this, client)

}