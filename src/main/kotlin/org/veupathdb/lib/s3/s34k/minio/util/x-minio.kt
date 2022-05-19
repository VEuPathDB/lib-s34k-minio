@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio.util

import io.minio.GetObjectResponse
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

internal inline fun GetObjectResponse.lastModified() =
  OffsetDateTime.parse(headers()["Last-Modified"], DateTimeFormatter.RFC_1123_DATE_TIME)

internal inline fun GetObjectResponse.eTag() =
  headers()["ETag"]!!