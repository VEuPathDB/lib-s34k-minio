@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio.util

import java.io.InputStream
import java.io.OutputStream
import java.util.stream.Stream
import java.util.stream.StreamSupport

internal inline fun <T> Iterable<T>.toStream(): Stream<T> = StreamSupport.stream(spliterator(), false)

internal inline fun <T> Stream<T>.toIterable(): Iterable<T> = StreamIterable(this)

internal class StreamIterable<T>(private val stream: Stream<T>): Iterable<T> {
  override fun iterator(): Iterator<T> {
    return stream.iterator()
  }
}

internal fun InputStream.pipeTo(out: OutputStream, bufSize: Int = 8192) {
  val buffer = ByteArray(bufSize)
  var red = 0

  do {
    red = read(buffer)
    out.write(buffer, 0, red)
  } while (red == bufSize)

  close()
  out.close()
}