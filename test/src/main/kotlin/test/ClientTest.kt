package test

import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.errors.BucketAlreadyExistsException
import org.veupathdb.lib.s3.s34k.errors.BucketNotFoundException
import org.veupathdb.lib.s3.s34k.params.bucket.BucketName

class ClientTest(private val client: S3Client) {

  private val Log = LoggerFactory.getLogger("ClientTest")

  fun run(): Result {
    val out = Result()

    out.add(bucketExistsNoBucket())
    out.add(testBucketExistsWithBucket())

    out.add(testBucketCreateWithConflict())
    out.add(testBucketCreateWithNoConflict())

    out.add(listBucketsWithNoBuckets())
    out.add(listBucketsWithBuckets())

    out.add(testCreateIfNotExistsWithConflict())
    out.add(testCreateIfNotExistsWithNoConflict())

    out.add(testGetBucketWithNoBucket())
    out.add(testGetBucketWithBucket())

    out.add(testDeleteBucketWithBucket())
    out.add(testDeleteBucketWithNoBucket())

    return out
  }

  // region Bucket Exists

  /**
   * Tests the [S3Client.bucketExists] method with no conflicting buckets in the
   * store.
   *
   * Test passes if `bucketExists` returns `false`.
   */
  private fun bucketExistsNoBucket(): Boolean {
    Log.info("Testing bucket exists check with non-existent bucket.")

    return ifNoBuckets {
      cat {
        val name = BucketName("no-bucket-here")

        if (client.bucketExists(name)) {
          fail("Bucket should not exist, but it does.")
        } else {
          succeed()
        }
      }
    }
  }

  /**
   * Tests the [S3Client.bucketExists] method with a matching bucket in the
   * store.
   *
   * Test passes if `bucketExists` returns `true`.
   */
  private fun testBucketExistsWithBucket(): Boolean {
    Log.info("Testing bucket exists check when the target bucket is present.")

    return ifNoBuckets {
      val name = BucketName("bar")

      cleanCat(name) {

        Log.debug("Creating bucket.")
        client.createBucket(name)

        if (!client.bucketExists(name)) {
          fail("Expected bucket to exist but it did not.")
        } else {
          succeed()
        }
      }
    }

  }

  // endregion

  // region List Buckets

  /**
   * Tests the [S3Client.listBuckets] method with no buckets in the store.
   *
   * Test passes if the `listBuckets` method returns an empty list.
   */
  private fun listBucketsWithNoBuckets(): Boolean {
    Log.info("Testing list buckets when no buckets exist.")

    return cat {
      if (client.listBuckets().isNotEmpty()) {
        fail("Store contains a non-zero number of buckets when zero buckets were expected.")
      } else {
        succeed()
      }
    }
  }

  /**
   * Tests the [S3Client.listBuckets] method with buckets in the store.
   *
   * Test passes if the `listBuckets` method returns a list containing only the
   * expected buckets.
   */
  private fun listBucketsWithBuckets(): Boolean {
    Log.info("Testing that listBuckets returns a list of all buckets in the store.")

    val name1 = BucketName("hello")
    val name2 = BucketName("goodbye")

    return ifNoBuckets {

      try {
        Log.debug("Putting bucket 1: $name1")
        client.createBucket(name1)

        Log.debug("Putting bucket 2: $name2")
        client.createBucket(name2)

        Log.debug("Retrieving bucket list")
        val list = client.listBuckets()

        if (list.size != 2)
          return fail("Bucket list did not contain exactly 2 entries.")

        list.forEach {
          if (it.name != name1 && it.name != name2)
            return fail("Bucket list contained a bucket with the unknown name ${it.name}")
        }

        succeed()
      } catch (e: Throwable) {
        Log.error("Test failed with exception!", e)
        fail("Exception thrown")
      } finally {
        cleanup(arrayOf(name1, name2))
      }
    }
  }

  // endregion

  // region Bucket Insert

  private fun testBucketCreateWithNoConflict(): Boolean {
    Log.info("Testing bucket creation.")

    val name = BucketName("foo")

    return ifNoBuckets {
      cleanCat(name) {
        Log.debug("Creating bucket.")
        client.createBucket(name)

        assertBucketExists(name)

        succeed()
      }
    }
  }

  private fun testBucketCreateWithConflict(): Boolean {
    Log.info("Testing bucket creation with a name conflict.")

    val name = BucketName("hello-world")

    return ifNoBuckets {
      cleanCat(name) {
        try {
          Log.debug("Creating bucket 1.")
          client.createBucket(name)

          Log.debug("Attempting to create bucket 2.")
          client.createBucket(name)

          fail("Expected BucketAlreadyExistsException to be thrown but it was not.")
        } catch (e: BucketAlreadyExistsException) {
          succeed()
        }
      }
    }
  }

  // endregion

  // region Bucket Upsert

  private fun testCreateIfNotExistsWithConflict(): Boolean {
    Log.info("Testing bucket upsert with a name conflict.")

    val name = BucketName("goodbye")

    return ifNoBuckets {
      cleanCat(name) {
        Log.debug("Creating bucket 1.")
        client.createBucket(name)

        Log.debug("Attempting to create bucket 2.")
        client.createBucketIfNotExists(name)

        if (client.listBuckets().size != 1) {
          fail("Expected store to contain exactly one bucket but it did not.")
        } else {
          succeed()
        }
      }
    }
  }

  private fun testCreateIfNotExistsWithNoConflict(): Boolean {
    Log.info("Testing bucket upsert with no name conflict.")

    val name = BucketName("nope")

    return ifNoBuckets {
      cleanCat(name) {
        Log.debug("Upsert new bucket.")
        client.createBucketIfNotExists(name)

        if (!client.bucketExists(name))
          fail("Expected bucket $name to exist, but it did not.")
        else
          succeed()
      }
    }
  }

  // endregion

  // region Get Bucket

  private fun testGetBucketWithNoBucket(): Boolean {
    Log.info("Testing that getBucket throws a BucketNotFound exception when requesting a non-existent bucket.")

    return ifNoBuckets {
      val name = BucketName("bucket-name")

      try {
        client.getBucket(name)
        fail("Expected getBucket to fail with a BucketNotFoundException but it did not.")
      } catch (e: BucketNotFoundException) {
        succeed()
      } catch (e: Throwable) {
        fail(e)
      }
    }
  }

  private fun testGetBucketWithBucket(): Boolean {
    Log.info("Testing that getBucket returns the expected bucket if it exists.")

    return ifNoBuckets {
      val name = BucketName("something")

      cleanCat(name) {

        Log.debug("Creating bucket {}", name)
        client.createBucket(name)

        val buck = client.getBucket(name)

        if (buck.name != name) {
          fail("Returned bucket did not match the created bucket.")
        } else {
          succeed()
        }
      }
    }
  }

  // endregion

  // region Delete Bucket

  private fun testDeleteBucketWithBucket(): Boolean {
    Log.info("Testing that deleteBucket removes a bucket if it exists")

    return ifNoBuckets {
      val name = BucketName("nothing")

      cleanCat(name) {
        Log.debug("Creating bucket $name")
        client.createBucket(name)

        Log.debug("Confirming bucket creation.")
        if (!client.bucketExists(name)) {
          return@cleanCat fail("Bucket '$name' does not exist when it should.")
        }

        Log.debug("Deleting bucket '$name'")
        client.deleteBucket(name)

        Log.debug("Confirming bucket deletion.")
        if (client.bucketExists(name)) {
          fail("Bucket '$name' still exists after delete call")
        } else {
          succeed()
        }
      }
    }
  }

  private fun testDeleteBucketWithNoBucket(): Boolean {
    Log.info("Testing that deleteBucket throws a BucketNotFound exception when there is no bucket to delete.")

    return ifNoBuckets {
      try {
        Log.debug("Attempting to delete non-existent bucket.")
        client.deleteBucket(BucketName("foobar"))
        fail("Expected deleteBucket to throw an exception but it did not.")
      } catch (e: BucketNotFoundException) {
        succeed()
      } catch (e: Throwable) {
        fail(e)
      }
    }
  }

  // endregion

  // test tag bucket
  // test remove bucket tags

  private fun assertBucketExists(name: BucketName) {
    Log.debug("Ensuring bucket $name exists.")
    if (!client.bucketExists(name)) {
      Log.error("Test condition failed!  Bucket $name does not exist!")
      throw RuntimeException("Tests failed.")
    }
  }

  private fun succeed(): Boolean {
    Log.info("Success!")
    return true
  }

  private fun fail(msg: String): Boolean {
    Log.error("Failed!  $msg")
    return false
  }

  private fun fail(err: Throwable): Boolean {
    Log.error("Failed with exception!", err)
    return false
  }

  private inline fun cat(action: () -> Boolean): Boolean {
    try {
      return action()
    } catch (e: Throwable) {
      Log.error("Test failed with exception!", e)
      throw e
    }
  }

  private inline fun cleanCat(name: BucketName, action: () -> Boolean): Boolean {
    try {
      return cat(action)
    } finally {
      cleanup(name)
    }
  }

  private inline fun ifNoBuckets(action: () -> Boolean): Boolean {
    Log.debug("Ensuring no buckets currently exist in the store.")

    if (client.listBuckets().isNotEmpty()) {
      Log.error("Test precondition failed!  Bucket list is not empty.")
      return false
    }

    return action()
  }

  private fun cleanup(name: BucketName) {
    cleanup(arrayOf(name))
  }

  private fun cleanup(names: Array<BucketName>) {
    for (name in names) {
      if (client.bucketExists(name)) {
        Log.debug("Performing test cleanup for bucket $name")
        client.deleteBucket(name)
      }
    }

    if (client.listBuckets().isNotEmpty()) {
      Log.error("Test cleanup failed! Bucket list is not empty.")
      throw RuntimeException("Tests failed.  Please cleanup minio image with 'docker-compose down'")
    }
  }
}