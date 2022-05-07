package org.veupathdb.lib.s3.s34k.minio

import io.minio.MinioClient
import org.veupathdb.lib.s3.s34k.BucketName
import org.veupathdb.lib.s3.s34k.core.AbstractBucket
import org.veupathdb.lib.s3.s34k.minio.operations.BucketDelete
import org.veupathdb.lib.s3.s34k.minio.operations.RecursiveBucketDeleter
import org.veupathdb.lib.s3.s34k.params.bucket.BucketDeleteParams
import org.veupathdb.lib.s3.s34k.params.bucket.recursive.RecursiveBucketDeleteParams
import java.time.OffsetDateTime

internal class MBucket(
  name:         BucketName,
  region:       String?,
  creationDate: OffsetDateTime,

  private val minio: MinioClient,
) : AbstractBucket(name, region, creationDate) {

  override val objects = BucketObjectContainer(this, minio)

  override val tags = BucketTagContainer(this, minio)

  override fun delete(params: BucketDeleteParams) =
    BucketDelete(name, region, params, minio)

  override fun deleteRecursive(params: RecursiveBucketDeleteParams) =
    RecursiveBucketDeleter(this.name, this.region, params, minio).execute()

  override fun toString() =
    "Bucket{ name='$name' }"
}