package org.veupathdb.lib.s3.s34k.minio

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.veupathdb.lib.s3.s34k.params.bucket.BucketExistsParams
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@DisplayName("util-to-minio.kt")
class UtilToMinioTest {

  @Nested
  @DisplayName("BucketExistsParams.toMinio()")
  inner class BucketExistsParamsToMinio {

    @Test
    @DisplayName("sets the bucket value on the minio args")
    fun t1() {
      val tgt = BucketExistsParams("hello").toMinio()
      assertEquals("hello", tgt.bucket())
    }

    @Test
    @DisplayName("sets the region value on the minio args")
    fun t2() {
      val tgt = BucketExistsParams("hello", "bye")
      assertEquals("bye", tgt.region)
    }

    @Test
    @DisplayName("sets the header values on the minio args")
    fun t3() {
      val inp = BucketExistsParams("hello", "bye")
      inp.addHeaders("foo", "bar", "fizz")

      val tgt = inp.toMinio()

      assertEquals(1, tgt.extraHeaders().asMap().size)
      assertEquals(2, tgt.extraHeaders().size())
      assertTrue(tgt.extraHeaders().containsKey("foo"))
      assertEquals(2, tgt.extraHeaders().get("foo").size)

      // convert to map since the collection may be unordered
      val mp = HashMap<String, Boolean>(2)

      tgt.extraHeaders().get("foo").forEach { mp[it] = true }

      assertNotNull(mp["bar"])
    }

    @Test
    @DisplayName("sets the queryParams values on the minio args")
    fun t4() {
      val inp = BucketExistsParams("hello", "bye")
      inp.addQueryParams("foo", "bar", "fizz")

      val tgt = inp.toMinio()

      assertEquals(1, tgt.extraQueryParams().asMap().size)
      assertEquals(2, tgt.extraQueryParams().size())
      assertTrue(tgt.extraQueryParams().containsKey("foo"))
      assertEquals(2, tgt.extraQueryParams().get("foo").size)

      // convert to map since the collection may be unordered
      val mp = HashMap<String, Boolean>(2)

      tgt.extraQueryParams().get("foo").forEach { mp[it] = true }

      assertNotNull(mp["bar"])
    }

  }
}