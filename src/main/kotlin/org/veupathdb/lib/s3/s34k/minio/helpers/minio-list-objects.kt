package org.veupathdb.lib.s3.s34k.minio.helpers

import io.minio.ListObjectsArgs
import io.minio.MinioClient
import io.minio.messages.Item
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.minio.toStream
import java.util.stream.Stream

private val Log = LoggerFactory.getLogger("MinioHelpers")

internal fun MinioClient.getObjectListStream(
  bucket: String,
  region: String?,
  pageSize: Int = 100,
): Stream<Item> {
  Log.trace("getObjectListStream(bucket = {}, region = {}, pageSize = {})", bucket, region, pageSize)

  return listObjects(ListObjectsArgs.builder()
    .bucket(bucket)
    .region(region)
    .recursive(true)
    .maxKeys(pageSize)
    .build())
    .toStream()
    .map { it.get() }
}