package org.veupathdb.lib.s3.s34k.minio

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@DisplayName("utils.kt")
class UtilsTest {

  @Nested
  @DisplayName("Map<String, Array<String>>.toMultiMap()")
  inner class ToMultiMap {

    @Test
    @DisplayName("properly converts the receiver map into a MultiMap instance")
    fun t1() {

      val input = mapOf(
        "test1" to arrayOf("a", "b"),
        "test2" to arrayOf("c"),
        "test3" to arrayOf("d", "e", "f")
      )

      val output = input.toMultiMap()

      assertEquals(6, output.size())
      assertEquals(3, output.asMap().size)

      assertTrue(output.containsKey("test1"))
      assertTrue(output.containsKey("test2"))
      assertTrue(output.containsKey("test3"))

      assertEquals(2, output.get("test1").size)
      assertEquals(1, output.get("test2").size)
      assertEquals(3, output.get("test3").size)

      var it = output.get("test1").iterator()

      assertEquals("a", it.next())
      assertEquals("b", it.next())

      it = output.get("test2").iterator()

      assertEquals("c", it.next())

      it = output.get("test3").iterator()

      assertEquals("d", it.next())
      assertEquals("e", it.next())
      assertEquals("f", it.next())
    }
  }

  @Nested
  @DisplayName("Map<K, V>.ifNotEmpty(fn)")
  inner class IfNotEmpty {

    @Test
    @DisplayName("calls the given function when the map is not empty")
    fun t1() {
      var foo: Map<String, String>? = null
      var count = 0

      val inp = mapOf("hi" to "bye")

      inp.ifNotEmpty {
        foo = it
        count++
      }

      assertEquals(1, count)
      assertNotNull(foo)
      assertEquals(1, foo!!.size)
      assertTrue(foo!!.containsKey("hi"))
      assertEquals("bye", foo!!["hi"])
    }

    @Test
    @DisplayName("does not call the given function when the map is empty")
    fun t2() {
      var count = 0

      emptyMap<String, String>().ifNotEmpty {
        count++
      }

      assertEquals(0, count)
    }
  }

  @Nested
  @DisplayName("String?.ifSet(fn)")
  inner class StringIfSet {

    @Test
    @DisplayName("does not call the given function if the receiver string is null")
    fun t1() {
      var count = 0

      (null as String?).ifSet { count++ }

      assertEquals(0, count)
    }

    @Test
    @DisplayName("does not call the given function if the receiver string is empty")
    fun t2() {
      var count = 0

      "".ifSet { count++ }

      assertEquals(0, count)
    }

    @Test
    @DisplayName("does not call the given function if the receiver string is blank")
    fun t3() {
      var count = 0

      "    ".ifSet { count++ }

      assertEquals(0, count)
    }

    @Test
    @DisplayName("calls the given function if the receiver string is not blank")
    fun t4() {
      var count = 0
      "hi".ifSet { count++ }
      assertEquals(1, count)
    }
  }
}