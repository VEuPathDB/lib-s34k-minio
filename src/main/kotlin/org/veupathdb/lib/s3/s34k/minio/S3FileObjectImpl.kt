package org.veupathdb.lib.s3.s34k.minio

import org.veupathdb.lib.s3.s34k.S3Bucket
import org.veupathdb.lib.s3.s34k.S3FileObject
import org.veupathdb.lib.s3.s34k.S3Headers
import java.io.File

internal open class S3FileObjectImpl(
  bucket:  S3Bucket,
  region:  String?,
  headers: S3Headers,
  path:    String,

  override val localFile: File
) : S3FileObject, S3ObjectImpl(bucket, region, headers, path)