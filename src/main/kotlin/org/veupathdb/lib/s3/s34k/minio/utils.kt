@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import com.google.common.collect.HashMultimap
import com.google.common.collect.Multimap


internal inline fun Map<String, Array<String>>.toMultiMap(): Multimap<String, String> {
  val out = HashMultimap.create<String, String>(size, 1)

  forEach { (k, v) -> out.putAll(k, v.asIterable()) }

  return out
}

internal inline fun <K, V, R> Map<K, V>.ifNotEmpty(fn: (Map<K, V>) -> R) {
  if (isNotEmpty()) fn(this)
}


internal inline fun <R> String?.ifSet(fn: (String) -> R) {
  if (this != null && this.isNotBlank())
    fn(this)
}