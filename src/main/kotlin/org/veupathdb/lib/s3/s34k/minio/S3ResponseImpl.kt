package org.veupathdb.lib.s3.s34k.minio

import org.veupathdb.lib.s3.s34k.S3Bucket
import org.veupathdb.lib.s3.s34k.S3Headers
import org.veupathdb.lib.s3.s34k.S3Response

sealed class S3ResponseImpl(
  final override val bucket:  S3Bucket,
  final override val region:  String?,
  final override val headers: S3Headers,
) : S3Response
