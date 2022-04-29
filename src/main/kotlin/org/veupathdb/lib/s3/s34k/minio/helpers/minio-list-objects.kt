package org.veupathdb.lib.s3.s34k.minio.helpers

import io.minio.ListObjectsArgs
import io.minio.MinioClient
import io.minio.messages.Item
import org.slf4j.LoggerFactory
import java.util.stream.Collectors
import java.util.stream.Stream
import java.util.stream.StreamSupport

private val Log = LoggerFactory.getLogger("MinioHelpers")

internal fun MinioClient.streamNonDeleteMarkerObjectNames(
  bucket: String,
  region: String?,
  pageSize: Int = 100,
): Stream<String> {
  Log.trace("listAllNonDeleteMarkers()")

  return getObjectListStream(bucket, region, pageSize)
    .filter { !it.isDeleteMarker }
    .map { it.objectName() }
}

internal fun MinioClient.getObjectListStream(
  bucket: String,
  region: String?,
  pageSize: Int = 100,
): Stream<Item> {
  Log.trace("getObjectListStream(bucket = {}, region = {}, pageSize = {})", bucket, region, pageSize)

  return StreamSupport.stream(
    listObjects(ListObjectsArgs.builder()
      .bucket(bucket)
      .region(region)
      .recursive(true)
      .maxKeys(pageSize)
      .build()).spliterator(),
    false
  ).map { it.get() }
}