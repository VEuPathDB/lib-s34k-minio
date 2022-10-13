package org.veupathdb.lib.s3.s34k.minio

import io.minio.MinioClient
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.S3Config
import org.veupathdb.lib.s3.s34k.minio.util.makeUrl

internal class MClient(config: S3Config) : S3Client {

  private val logger = LoggerFactory.getLogger(javaClass)

  private val client: MinioClient

  init {
    val url = config.makeUrl()

    logger.debug("Creating MinIO client for url: $url")

    client = MinioClient.builder()
      .region(config.region)
      .endpoint(url)
      .credentials(config.accessKey, config.secretKey)
      .build()
  }

  override val defaultRegion = config.region

  override val buckets = BucketContainer(this, client)

}