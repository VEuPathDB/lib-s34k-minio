@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import com.google.common.collect.HashMultimap
import com.google.common.collect.Multimap
import io.minio.errors.ErrorResponseException
import io.minio.errors.MinioException
import jdk.internal.org.jline.utils.Log
import org.slf4j.Logger
import org.veupathdb.lib.s3.s34k.errors.S34kException
import org.veupathdb.lib.s3.s34k.params.bucket.BucketName


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

internal inline fun <C : (A) -> Unit, A> C?.invoke(fName: String, log: Logger, arg: A): A {
  if (this != null) {
    log.debug("Executing callback {} in $fName")
    this(arg)
  }

  return arg
}

internal inline fun <C : () -> Unit> C?.invoke(fName: String, log: Logger) {
  if (this != null) {
    log.debug("Executing callback {} in $fName")
    this()
  }
}