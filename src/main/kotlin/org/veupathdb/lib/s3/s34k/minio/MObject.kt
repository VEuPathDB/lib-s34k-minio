package org.veupathdb.lib.s3.s34k.minio

import io.minio.MinioClient
import io.minio.RemoveObjectArgs
import io.minio.StatObjectArgs
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.Bucket
import org.veupathdb.lib.s3.s34k.ObjectMeta
import org.veupathdb.lib.s3.s34k.core.AbstractS3Object
import org.veupathdb.lib.s3.s34k.core.BasicObjectMeta
import org.veupathdb.lib.s3.s34k.fields.Headers
import org.veupathdb.lib.s3.s34k.minio.fields.MHeaders
import org.veupathdb.lib.s3.s34k.minio.operations.ObjectDelete
import org.veupathdb.lib.s3.s34k.minio.operations.ObjectExists
import org.veupathdb.lib.s3.s34k.minio.operations.StatObject
import org.veupathdb.lib.s3.s34k.minio.util.*
import org.veupathdb.lib.s3.s34k.params.DeleteParams
import org.veupathdb.lib.s3.s34k.params.ExistsParams
import org.veupathdb.lib.s3.s34k.params.`object`.ObjectExistsParams
import org.veupathdb.lib.s3.s34k.params.`object`.ObjectStatParams

internal open class MObject(
  path:    String,
  region:  String?,
  headers: Headers,
  bucket:  Bucket,

  private val client: MinioClient,
) : AbstractS3Object(path, region, headers, bucket) {

  private val log = LoggerFactory.getLogger(this::class.java)

  // suppressed because ObjectTagContainer constructor doesn't do anything with
  // the reference, so it is safe to pass down.
  @Suppress("LeakingThis")
  override val tags: ObjectTagContainer = ObjectTagContainer(this, client)


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