package org.veupathdb.lib.s3.s34k.minio

import io.minio.messages.Tags
import okhttp3.internal.toImmutableMap
import org.veupathdb.lib.s3.s34k.S3Tag
import org.veupathdb.lib.s3.s34k.S3TagSet

internal class S3TagSetImpl(private val raw: Map<String, String>) : S3TagSet {

  constructor(tags: Tags) : this(tags.get())

  override val isEmpty get() = raw.isEmpty()

  override val isNotEmpty get() = raw.isNotEmpty()

  override val size get() = raw.size

  override fun asSet(): Set<S3Tag> {
    val out = HashSet<S3Tag>(raw.size)
    raw.forEach { (k, v) -> out.add(S3Tag(k, v)) }
    return out
  }

  override fun asList(): List<S3Tag> {
    val out = ArrayList<S3Tag>(raw.size)
    raw.forEach { (k, v) -> out.add(S3Tag(k, v)) }
    return out
  }

  override fun asMap(): Map<String, String> = raw.toImmutableMap()

  override fun iterator(): Iterator<S3Tag> =
    raw.entries.stream()
      .map { (k, v) -> S3Tag(k, v) }
      .iterator()
}