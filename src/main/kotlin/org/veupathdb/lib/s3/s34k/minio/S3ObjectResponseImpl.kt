package org.veupathdb.lib.s3.s34k.minio

import org.veupathdb.lib.s3.s34k.S3Bucket
import org.veupathdb.lib.s3.s34k.S3Headers
import org.veupathdb.lib.s3.s34k.S3ObjectResponse

internal sealed class S3ObjectResponseImpl(
                     bucket:  S3Bucket,
                     region:  String?,
                     headers: S3Headers,
  final override val path:    String,
) : S3ObjectResponse, S3ResponseImpl(bucket, region, headers)