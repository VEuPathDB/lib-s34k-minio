@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio.util

import com.google.common.collect.HashMultimap
import com.google.common.collect.Multimap
import org.veupathdb.lib.s3.s34k.fields.Headers
import org.veupathdb.lib.s3.s34k.fields.QueryParams
import org.veupathdb.lib.s3.s34k.util.ListMap

internal inline fun Headers.toMultiMap(): Multimap<String, String> =
  HashMultimap.create<String, String>(size.toInt(), 2).also {
    forEach { (k, v) -> it.putAll(k, v) }
  }

internal inline fun QueryParams.toMultiMap(): Multimap<String, String> =
  HashMultimap.create<String, String>(size.toInt(), 2).also {
    forEach { (k, v) -> it.putAll(k, v) }
  }

/**
 * Cascade apply headers / query params.
 *
 * First append all the headers / query params from the first set, then apply
 * the second set, overwriting anything that was added by the first.
 */
internal inline fun multiMap(a: ListMap<String, String>, b: ListMap<String, String>) =
  HashMultimap.create<String, String>((a.size + b.size).toInt(), 2).also {
    a.forEach { (k, v) -> it.putAll(k, v) }
    b.forEach { (k, v) ->
      if (it.containsKey(k)) {
        it.removeAll(k)
      }

      it.putAll(k, v)
    }
  }
