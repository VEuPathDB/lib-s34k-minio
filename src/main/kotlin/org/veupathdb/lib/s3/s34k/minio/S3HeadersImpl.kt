package org.veupathdb.lib.s3.s34k.minio

import okhttp3.Headers
import org.veupathdb.lib.s3.s34k.S3Headers
import java.util.*

internal class S3HeadersImpl(head: Headers) : S3Headers {

  private val raw: Map<String, List<String>>

  override val size get() = raw.size

  init {
    raw = head.toMultimap()
  }

  override fun getHeader(key: String) = raw[key]?.get(0)

  override fun getHeaders(key: String) = raw[key]

  override fun keys(): Set<String> = raw.keys

  override fun toMap(): Map<String, List<String>> =
    Collections.unmodifiableMap(raw)

  override fun iterator(): Iterator<Pair<String, List<String>>> =
    raw.entries.stream()
      .map { (k, v) -> Pair(k, v) }
      .iterator()
}