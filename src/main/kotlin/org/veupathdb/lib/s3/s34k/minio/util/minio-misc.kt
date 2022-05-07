@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio.util

import io.minio.MinioClient
import io.minio.messages.Bucket
import org.veupathdb.lib.s3.s34k.BucketName
import org.veupathdb.lib.s3.s34k.minio.MBucket

internal inline fun List<Bucket>.hunt(
  name:   BucketName,
  region: String?,
  minio:  MinioClient
): org.veupathdb.lib.s3.s34k.Bucket? {
  return stream()
    .filter { it.name() == name.name }
    .findFirst()
    .map { MBucket(name, region, it.creationDate().toOffsetDateTime(), minio) }
    .orElse(null)
}
