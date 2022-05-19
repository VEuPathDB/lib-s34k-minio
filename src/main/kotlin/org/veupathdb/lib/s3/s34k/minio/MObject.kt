package org.veupathdb.lib.s3.s34k.minio

import io.minio.MinioClient
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.buckets.S3Bucket
import org.veupathdb.lib.s3.s34k.core.objects.AbstractS3Object
import org.veupathdb.lib.s3.s34k.fields.Headers
import org.veupathdb.lib.s3.s34k.minio.operations.ObjectDelete
import org.veupathdb.lib.s3.s34k.minio.operations.ObjectExists
import org.veupathdb.lib.s3.s34k.minio.operations.StatObject
import org.veupathdb.lib.s3.s34k.objects.ObjectMeta
import org.veupathdb.lib.s3.s34k.params.DeleteParams
import org.veupathdb.lib.s3.s34k.params.`object`.ObjectExistsParams
import org.veupathdb.lib.s3.s34k.params.`object`.ObjectStatParams
import java.time.OffsetDateTime

internal open class MObject(
  path:         String,
  lastModified: OffsetDateTime?,
  eTag:         String,
  region:       String?,
  headers:      Headers,
  bucket:       S3Bucket,

  private val client: MinioClient,
) : AbstractS3Object(path, lastModified, eTag, region, headers, bucket) {

  private val log = LoggerFactory.getLogger(this::class.java)

  // suppressed because ObjectTagContainer constructor doesn't do anything with
  // the reference, so it is safe to pass down.
  @Suppress("LeakingThis")
  override val tags = ObjectTagContainer(this, client)


  override fun delete(params: DeleteParams) {
    log.debug("Attempting to delete '{}'", this)
    ObjectDelete(bucket, path, params, client)
  }


  override fun exists(params: ObjectExistsParams): Boolean {
    log.debug("Attempting to test existence '{}'", this)
    return ObjectExists(bucket, path, params, client)
  }


  override fun stat(params: ObjectStatParams): ObjectMeta? {
    log.debug("Attempting to stat '{}'", this)
    return StatObject(bucket, path, params, client)
  }

  override fun toString() = "Object{ path='$path', bucket='${bucket.name}' }"
}