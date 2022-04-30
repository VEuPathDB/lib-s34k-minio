@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import com.google.common.collect.HashMultimap
import com.google.common.collect.Multimap
import io.minio.BaseArgs
import io.minio.BucketArgs
import io.minio.messages.DeleteObject
import org.slf4j.Logger
import org.veupathdb.lib.s3.s34k.fields.BucketName
import org.veupathdb.lib.s3.s34k.fields.S3PathSet
import org.veupathdb.lib.s3.s34k.fields.headers.S3Headers
import org.veupathdb.lib.s3.s34k.fields.query_params.S3QueryParams
import org.veupathdb.lib.s3.s34k.requests.S3RegionRequestParams
import org.veupathdb.lib.s3.s34k.response.bucket.S3Bucket
import java.util.stream.StreamSupport

/**
 * Dummy Iterable
 *
 * Simple type that wraps an iterator.  Basic use is to wrap a java Stream's
 * iterator for use in for loops and other places where an [Iterable] instance
 * is desired.
 */
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


// region MinIO Builder Extensions

/**
 * Simple bucket setter that unwraps [BucketName] instances.
 */
internal inline fun <B : BucketArgs.Builder<B, A>, A : BucketArgs> B.bucket(name: BucketName) = bucket(name.name)
internal inline fun <B : BucketArgs.Builder<B, A>, A : BucketArgs> B.bucket(bucket: S3Bucket) = bucket(bucket.bucketName)

/**
 * Region setter that chooses the first non-null value to set as the builder's
 * region value.
 */
internal inline fun <B : BucketArgs.Builder<B, A>, A : BucketArgs> B.regions(vararg regions: String?) =
  also {
    for (r in regions) {
      if (r.isNullOrBlank())
        continue;

      region(r)
      break
    }
  }
internal inline fun <B : BucketArgs.Builder<B, A>, A : BucketArgs> B.region(
  params: S3RegionRequestParams,
  bucket: S3Bucket
) = region(params.region ?: bucket.defaultRegion ?: bucket.client.defaultRegion)

internal inline fun <B : BaseArgs.Builder<B, A>, A : BaseArgs> B.headers(headers: S3Headers) =
  also { it.extraHeaders(headers.toMultiMap()) }

internal inline fun <B : BaseArgs.Builder<B, A>, A : BaseArgs> B.queryParams(queryParams: S3QueryParams) =
  also { it.extraQueryParams(queryParams.toMultiMap()) }

// endregion MinIO Builder Extensions


/**
 * Ensures that the receiver string ends with a '/' character.
 */
internal inline fun String.asPath() = if (endsWith('/')) this else "$this/"


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

internal inline fun <T> Iterable<T>.toStream() = StreamSupport.stream(spliterator(), false)

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