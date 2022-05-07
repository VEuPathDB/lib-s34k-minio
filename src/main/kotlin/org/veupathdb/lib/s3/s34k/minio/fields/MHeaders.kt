package org.veupathdb.lib.s3.s34k.minio.fields

import org.veupathdb.lib.s3.s34k.core.fields.BasicHeaders
import org.veupathdb.lib.s3.s34k.fields.Headers
import okhttp3.Headers as HTTPHeaders

internal class MHeaders : Headers, BasicHeaders {
  constructor(head: HTTPHeaders) : super(head.toMultimap())

  constructor()
}