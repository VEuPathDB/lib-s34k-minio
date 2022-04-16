@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import io.minio.messages.LegalHold
import io.minio.messages.RetentionMode
import org.veupathdb.lib.s3.s34k.S3LegalHold
import org.veupathdb.lib.s3.s34k.S3RetentionMode


internal inline fun LegalHold.fromMinio() =
  if (status())
    S3LegalHold.On
  else
    S3LegalHold.Off

internal inline fun RetentionMode.fromMinio() = when(this) {
  RetentionMode.GOVERNANCE -> S3RetentionMode.Governance
  RetentionMode.COMPLIANCE -> S3RetentionMode.Compliance
}
