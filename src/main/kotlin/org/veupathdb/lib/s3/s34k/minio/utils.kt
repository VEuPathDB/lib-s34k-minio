@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import com.google.common.collect.HashMultimap
import com.google.common.collect.Multimap
import io.minio.BaseArgs
import io.minio.BucketArgs
import io.minio.messages.DeleteObject
import org.slf4j.Logger
import org.veupathdb.lib.s3.s34k.fields.S3PathSet
import org.veupathdb.lib.s3.s34k.fields.headers.S3Headers
import org.veupathdb.lib.s3.s34k.fields.query_params.S3QueryParams
import java.util.Base64
import java.util.Collections

internal class DummyIterable<T>(
  private val it: Iterator<T>,
  private var reusable: Boolean = true,
): Iterable<T> {
  private var used = false

  override fun iterator() = if (used && !reusable)
    throw IllegalStateException("Attempted to reuse one-time-use iterable")
  else {
    used = true
    it
  }
}

internal inline fun <B : BucketArgs.Builder<B, A>, A : BucketArgs> B.regions(vararg regions: String?) =
  also {
    for (r in regions) {
      if (r.isNullOrBlank())
        continue;

      region(r)
      break
    }
  }

internal inline fun <B : BaseArgs.Builder<B, A>, A : BaseArgs> B.headers(headers: S3Headers) =
  also { it.extraHeaders(headers.toMultiMap()) }

internal inline fun <B : BaseArgs.Builder<B, A>, A : BaseArgs> B.queryParams(queryParams: S3QueryParams) =
  also { it.extraQueryParams(queryParams.toMultiMap()) }


internal inline fun S3Headers.toMultiMap(): Multimap<String, String> {
  val out = HashMultimap.create<String, String>(size, 1)
  forEach { (k, v) -> out.putAll(k, v.asIterable()) }
  return out
}

internal inline fun S3Headers.toMultiMap(global: S3Headers): Multimap<String, String> {
  val out = HashMultimap.create<String, String>(size + global.size, 1)
  global.forEach { (k, v) -> out.putAll(k, v.asIterable()) }
  forEach { (k, v) -> out.putAll(k, v.asIterable()) }
  return out
}

internal inline fun S3QueryParams.toMultiMap(): Multimap<String, String> {
  val out = HashMultimap.create<String, String>(size, 1)
  forEach { (k, v) -> out.putAll(k, v.asIterable()) }
  return out
}

internal inline fun S3QueryParams.toMultiMap(global: S3QueryParams): Multimap<String, String> {
  val out = HashMultimap.create<String, String>(size + global.size, 1)
  global.forEach { (k, v) -> out.putAll(k, v.asIterable()) }
  forEach { (k, v) -> out.putAll(k, v.asIterable()) }
  return out
}

internal inline fun S3PathSet.toDelObjList() : Iterable<DeleteObject> {
  val set = toSet()
  val out = ArrayList<DeleteObject>(set.size)

  set.forEach { out.add(DeleteObject(it)) }

  return out
}

internal inline fun <K, V, R> Map<K, V>.ifNotEmpty(fn: (Map<K, V>) -> R) {
  if (isNotEmpty()) fn(this)
}

internal inline fun <K, V> Map<K, V>.immutable(): Map<K, V> =
  Collections.unmodifiableMap(this)

internal inline fun <E> List<E>.immutable(): List<E> =
  Collections.unmodifiableList(this)

internal inline fun <E> Set<E>.immutable(): Set<E> =
  Collections.unmodifiableSet(this)

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