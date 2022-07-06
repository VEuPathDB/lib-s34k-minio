package org.veupathdb.lib.s3.s34k.minio.util

import org.veupathdb.lib.s3.s34k.S3Config
import org.veupathdb.lib.s3.s34k.errors.InvalidRequestConfigError
import org.veupathdb.lib.s3.s34k.params.S3RequestParams
import org.veupathdb.lib.s3.s34k.params.`object`.FileUploadParams
import org.veupathdb.lib.s3.s34k.params.`object`.ObjectDownloadParams
import org.veupathdb.lib.s3.s34k.params.`object`.StreamingObjectPutParams

internal fun ObjectDownloadParams.reqFile() =
  localFile ?: raise("localFile", this)

internal fun StreamingObjectPutParams.reqStream() =
  stream ?: raise("stream", this)

internal fun S3Config.makeUrl() =
  if (secure) {
    if (port > 0u)
      "https://$url:$port"
    else
      "https://$url"
  } else {
    if (port > 0u)
      "http://$url:$port"
    else
      "http://$url"
  }


internal fun raise(field: String, params: S3RequestParams): Nothing =
  throw InvalidRequestConfigError("Required field '$field' was not set.", params)

internal fun FileUploadParams.reqFile() =
  (localFile ?: raise("localFile", this)).also {
    if (it.isDirectory)
      throw InvalidRequestConfigError("Input file $localFile is a directory not a normal file", this)

    if (!it.exists())
      throw InvalidRequestConfigError("File $localFile does not exist", this)

    if (!it.canRead())
      throw InvalidRequestConfigError("Cannot read file $localFile", this)
  }