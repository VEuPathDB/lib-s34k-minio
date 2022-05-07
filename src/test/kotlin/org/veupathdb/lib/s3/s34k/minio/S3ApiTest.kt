package org.veupathdb.lib.s3.s34k.minio

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.veupathdb.lib.s3.s34k.S3Api
import org.veupathdb.lib.s3.s34k.S3Config
import kotlin.test.assertTrue

@DisplayName("S3Api")
internal class S3ApiTest {

  @Nested
  @DisplayName("newClient(config)")
  inner class NewClient {

    @Test
    @DisplayName("Locates the S3ClientProvideImpl class.")
    fun t1() {
      val conf   = S3Config("foo", 123u, false, "fizz", "buzz", "taco")
      val client = assertDoesNotThrow {
        S3Api.newClient(conf)
      }

      assertTrue(client is MClient)
      assertTrue(client.defaultRegion == "taco")
    }
  }
}