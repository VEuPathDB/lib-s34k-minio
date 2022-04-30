package test

import io.minio.ListObjectsArgs
import io.minio.MakeBucketArgs
import io.minio.MinioClient
import io.minio.PutObjectArgs
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.errors.BucketAlreadyExistsException
import org.veupathdb.lib.s3.s34k.errors.BucketNotEmptyException
import org.veupathdb.lib.s3.s34k.errors.BucketNotFoundException
import org.veupathdb.lib.s3.s34k.fields.BucketName
import java.io.ByteArrayInputStream

class ClientTest(
  private val minio:  MinioClient,
  private val client: S3Client,
) {

  private val Log = LoggerFactory.getLogger("ClientTest")

  fun run(): Result {
    val out = Result()

    // TODO: Test with regions

    Log.info("Client.bucketExists when target bucket is not present")
    out.add(bucketExistsNoBucket().withTakeDown())
    Log.info("Client.bucketExists when target bucket is present")
    out.add(testBucketExistsWithBucket().withTakeDown())

    Log.info("Client.createBucket with name conflicting with an existing bucket")
    out.add(testBucketCreateWithConflict().withTakeDown())
    Log.info("Client.createBucket with no name conflicts")
    out.add(testBucketCreateWithNoConflict().withTakeDown())

    Log.info("Client.listBuckets when the store has no buckets")
    out.add(listBucketsWithNoBuckets().withTakeDown())
    Log.info("Client.listBuckets when the store has buckets")
    out.add(listBucketsWithBuckets().withTakeDown())

    Log.info("Client.createIfNotExists with name conflicting with an existing bucket")
    out.add(testCreateIfNotExistsWithConflict().withTakeDown())
    Log.info("Client.createIfNotExists with no name conflicts")
    out.add(testCreateIfNotExistsWithNoConflict().withTakeDown())

    Log.info("Client.getBucket requesting a bucket that does not exist")
    out.add(testGetBucketWithNoBucket().withTakeDown())
    Log.info("Client.getBucket requesting a bucket that does exist")
    out.add(testGetBucketWithBucket().withTakeDown())

    Log.info("Client.deleteBucket when the target bucket does not exist")
    out.add(testDeleteBucketWithNoBucket().withTakeDown())
    Log.info("Client.deleteBucket when the target bucket exists and is empty")
    out.add(testDeleteBucketWithBucket().withTakeDown())
    Log.info("Client.deleteBucket when the target bucket exists but is not empty")
    out.add(testDeleteBucketWhenNotEmpty().withTakeDown())

    Log.info("Client.deleteBucketRecursive when the target bucket does not exist")
    out.add(testRecursiveDeleteWithNoBucket().withTakeDown())
    Log.info("Client.deleteBucketRecursive when the target bucket does exist and is empty")
    out.add(testRecursiveDeleteWithEmptyBucket().withTakeDown())
    Log.info("Client.deleteBucketRecursive when the target bucket does exist but is not empty")
    out.add(testRecursiveDeleteWithNonEmptyBucket().withTakeDown())

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
    return ifNoBuckets {
      cat {
        val name = BucketName("no-bucket-here")

        if (client.bucketExists(name)) {
          Log.fail("Bucket should not exist, but it does.")
        } else {
          Log.succeed()
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
    return ifNoBuckets {
      val name = BucketName("bar")

      cleanCat(name) {

        Log.debug("Creating bucket.")
        client.createBucket(name)

        if (!client.bucketExists(name)) {
          Log.fail("Expected bucket to exist but it did not.")
        } else {
          Log.succeed()
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
    return cat {
      if (client.listBuckets().isNotEmpty) {
        Log.fail("Store contains a non-zero number of buckets when zero buckets were expected.")
      } else {
        Log.succeed()
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
          return Log.fail("Bucket list did not contain exactly 2 entries.")

        list.forEach {
          if (it.bucketName != name1 && it.bucketName != name2)
            return Log.fail("Bucket list contained a bucket with the unknown name ${it.bucketName}")
        }

        Log.succeed()
      } catch (e: Throwable) {
        Log.error("Test failed with exception!", e)
        Log.fail("Exception thrown")
      } finally {
        cleanup(arrayOf(name1, name2))
      }
    }
  }

  // endregion

  // region Bucket Insert

  private fun testBucketCreateWithNoConflict(): Boolean {
    val name = BucketName("foo")

    return ifNoBuckets {
      cleanCat(name) {
        Log.debug("Creating bucket.")
        client.createBucket(name)

        assertBucketExists(name)

        Log.succeed()
      }
    }
  }

  private fun testBucketCreateWithConflict(): Boolean {
    val name = BucketName("hello-world")

    return ifNoBuckets {
      cleanCat(name) {
        try {
          Log.debug("Creating bucket 1.")
          client.createBucket(name)

          Log.debug("Attempting to create bucket 2.")
          client.createBucket(name)

          Log.fail("Expected BucketAlreadyExistsException to be thrown but it was not.")
        } catch (e: BucketAlreadyExistsException) {
          Log.succeed()
        }
      }
    }
  }

  // endregion

  // region Bucket Upsert

  private fun testCreateIfNotExistsWithConflict(): Boolean {
    val name = BucketName("goodbye")

    return ifNoBuckets {
      cleanCat(name) {
        Log.debug("Creating bucket 1.")
        client.createBucket(name)

        Log.debug("Attempting to create bucket 2.")
        client.createBucketIfNotExists(name)

        if (client.listBuckets().size != 1) {
          Log.fail("Expected store to contain exactly one bucket but it did not.")
        } else {
          Log.succeed()
        }
      }
    }
  }

  private fun testCreateIfNotExistsWithNoConflict(): Boolean {
    val name = BucketName("nope")

    return ifNoBuckets {
      cleanCat(name) {
        Log.debug("Upsert new bucket.")
        client.createBucketIfNotExists(name)

        if (!client.bucketExists(name))
          Log.fail("Expected bucket $name to exist, but it did not.")
        else
          Log.succeed()
      }
    }
  }

  // endregion

  // region Get Bucket

  private fun testGetBucketWithNoBucket(): Boolean {
    return ifNoBuckets {
      val name = BucketName("bucket-name")

      try {
        client.getBucket(name)
        Log.fail("Expected getBucket to fail with a BucketNotFoundException but it did not.")
      } catch (e: BucketNotFoundException) {
        Log.succeed()
      } catch (e: Throwable) {
        Log.fail(e)
      }
    }
  }

  private fun testGetBucketWithBucket(): Boolean {
    return ifNoBuckets {
      val name = BucketName("something")

      cleanCat(name) {

        Log.debug("Creating bucket {}", name)
        client.createBucket(name)

        val buck = client.getBucket(name)

        if (buck.bucketName != name) {
          Log.fail("Returned bucket did not match the created bucket.")
        } else {
          Log.succeed()
        }
      }
    }
  }

  // endregion

  // region Delete Bucket

  private fun testDeleteBucketWithBucket(): Boolean {
    Log.debug("Setup: ensure no buckets currently exist")
    try {
      if (minio.listBuckets().isNotEmpty())
        return Log.fail("Test precondition failed, bucket list is not empty.")
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    val name = "bucket-to-delete"

    Log.debug("Setup: creating bucket '{}'", name)
    try {
      minio.makeBucket(name)
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    Log.debug("Attempting to delete bucket '{}'", name)
    try {
      if (!client.deleteBucket(BucketName(name)))
        return Log.fail("Expected deleteBucket to return true but it returned false")
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    return Log.succeed()
  }

  private fun testDeleteBucketWithNoBucket(): Boolean {
    Log.debug("Setup: ensure no buckets currently exist")
    try {
      if (minio.listBuckets().isNotEmpty())
        return Log.fail("Test precondition failed, bucket list is not empty.")
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    Log.debug("Attempting to delete non-existent bucket.")
    return try {
      if (client.deleteBucket(BucketName("foobar")))
        Log.fail("Expected deleteBucket to return false but it returned true")
      else
        Log.succeed()
    } catch (e: Throwable) {
      Log.fail(e)
    }
  }

  private fun testDeleteBucketWhenNotEmpty(): Boolean {
    val bucketName = "delete-bucket-not-empty"

    Log.debug("Setup: ensure no buckets currently exist")
    try {
      if (minio.listBuckets().isNotEmpty())
        return Log.fail("Test precondition failed, bucket list is not empty.")
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    Log.debug("Setup: create test bucket")
    try {
      minio.makeBucket(MakeBucketArgs.builder()
        .bucket(bucketName)
        .build())
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    Log.debug("Setup: populate test bucket")
    try {
      minio.putObject(PutObjectArgs.builder()
        .bucket(bucketName)
        .`object`("some/object/key")
        .contentType("application/json")
        .stream(ByteArrayInputStream("false".toByteArray()), 5, -1)
        .build())
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    Log.debug("Attempting bucket removal")
    return try {
      client.deleteBucket(BucketName(bucketName))
      Log.fail("No exception was thrown.")
    } catch (e: BucketNotEmptyException) {
      Log.succeed()
    } catch (e: Throwable) {
      Log.fail(e)
    }
  }

  // endregion Delete Bucket

  // region Recursive Bucket Delete

  private fun testRecursiveDeleteWithNoBucket(): Boolean {
    Log.debug("Setup: Ensure no buckets currently exist.")
    try {
      if (minio.listBuckets().isNotEmpty())
        return Log.fail("Test precondition failed: buckets already exist in the store")
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    Log.debug("Attempting to delete non-existent bucket.")
    try {
      if (client.deleteBucketRecursive(BucketName("something")))
        return Log.fail("deleteBucketRecursive returned true when false was expected")
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    return Log.succeed()
  }

  private fun testRecursiveDeleteWithEmptyBucket(): Boolean {
    Log.debug("Setup: Ensure no buckets currently exist.")
    try {
      if (minio.listBuckets().isNotEmpty())
        return Log.fail("Test precondition failed: buckets already exist in the store")
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    val bucketName = "some-bucket"

    Log.debug("Setup: Creating bucket '{}'", bucketName)
    try {
      minio.makeBucket(bucketName)
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    Log.debug("Attempting to delete empty bucket '{}'", bucketName)
    try {
      if (!client.deleteBucketRecursive(BucketName(bucketName)))
        return Log.fail("deleteBucketRecursive returned false when true was expected")
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    return Log.succeed()
  }

  private fun testRecursiveDeleteWithNonEmptyBucket(): Boolean {
    Log.debug("Setup: Ensure no buckets currently exist.")
    try {
      if (minio.listBuckets().isNotEmpty())
        return Log.fail("Test precondition failed: buckets already exist in the store")
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    val bucketName = "my-bucket"

    Log.debug("Setup: Creating bucket '{}'", bucketName)
    try {
      minio.makeBucket(bucketName)
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    Log.debug("Setup: populating bucket '{}'", bucketName)
    try {
      minio.makeObjects(bucketName,
        "foo"   to "bar",
        "fizz"  to "buzz",
        "happy" to "sad",
        "hello" to "goodbye",
        "cruel" to "world",
        "69"    to "666",
      )
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    Log.debug("Setup: ensuring object existence for bucket '{}'", bucketName)
    try {
      val res = minio.listObjects(ListObjectsArgs.builder()
        .bucket(bucketName)
        .build())

      var counter = 0

      res.forEach { _ -> counter++ }

      if (counter != 6)
        return Log.fail("Test precondition failed, not all objects were created.")
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    Log.debug("Attempting to recursively delete non-empty bucket '{}'", bucketName)
    try {
      if (!client.deleteBucketRecursive(BucketName(bucketName)))
        return Log.fail("deleteBucketRecursive returned false when true was expected")
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    Log.debug("Verifying bucket was deleted.")
    try {
      if (minio.listBuckets().isNotEmpty())
        return Log.fail("Bucket was not deleted when it should have been")
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    return Log.succeed()
  }

  // endregion Recursive Bucket Delete

  // region Helpers

  private fun Boolean.withTakeDown() = also { minio.clearBuckets() }

  private fun assertBucketExists(name: BucketName) {
    Log.debug("Ensuring bucket $name exists.")
    if (!client.bucketExists(name)) {
      Log.error("Test condition failed!  Bucket $name does not exist!")
      throw RuntimeException("Tests failed.")
    }
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

    if (client.listBuckets().isNotEmpty) {
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

    if (client.listBuckets().isNotEmpty) {
      Log.error("Test cleanup failed! Bucket list is not empty.")
      throw RuntimeException("Tests failed.  Please cleanup minio image with 'docker-compose down'")
    }
  }

  // endregion
}