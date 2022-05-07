@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio.util

import org.veupathdb.lib.s3.s34k.LegalHold
import org.veupathdb.lib.s3.s34k.RetentionMode
import io.minio.messages.LegalHold as MHold
import io.minio.messages.RetentionMode as MRMode

internal inline fun MHold.toS34K() = if (status()) LegalHold.On else LegalHold.Off

internal inline fun MRMode.toS34K() =
  when (this) {
    MRMode.GOVERNANCE -> RetentionMode.Governance
    MRMode.COMPLIANCE -> RetentionMode.Compliance
  }
