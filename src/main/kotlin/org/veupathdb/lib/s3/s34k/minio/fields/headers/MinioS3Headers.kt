package org.veupathdb.lib.s3.s34k.minio.fields.headers

import okhttp3.Headers
import org.veupathdb.lib.s3.s34k.core.fields.headers.BasicS3Headers

class MinioS3Headers(values: Headers) : BasicS3Headers() {
  init { values.toMultimap().forEach { (k, v) -> raw[k] = v.toTypedArray() } }
}