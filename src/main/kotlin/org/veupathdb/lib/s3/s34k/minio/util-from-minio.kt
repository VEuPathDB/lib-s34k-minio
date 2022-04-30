@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import io.minio.messages.Item
import io.minio.messages.LegalHold
import io.minio.messages.RetentionMode
import org.veupathdb.lib.s3.s34k.S3LegalHold
import org.veupathdb.lib.s3.s34k.S3RetentionMode
import org.veupathdb.lib.s3.s34k.core.fields.headers.BasicS3Headers
import org.veupathdb.lib.s3.s34k.core.response.`object`.BasicS3Object
import org.veupathdb.lib.s3.s34k.response.bucket.S3Bucket
import org.veupathdb.lib.s3.s34k.response.`object`.S3Object


internal inline fun LegalHold.fromMinio() =
  if (status())
    S3LegalHold.On
  else
    S3LegalHold.Off

internal inline fun RetentionMode.fromMinio() = when(this) {
  RetentionMode.GOVERNANCE -> S3RetentionMode.Governance
  RetentionMode.COMPLIANCE -> S3RetentionMode.Compliance
}

internal inline fun Item.toS3Object(bucket: S3Bucket) : S3Object {
  return BasicS3Object(
    bucket,
    BasicS3Headers(),
    bucket.defaultRegion,
    objectName()
  )
}