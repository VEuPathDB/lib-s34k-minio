package test

import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.errors.BucketAlreadyExistsException
import org.veupathdb.lib.s3.s34k.params.bucket.BucketName

class ClientTest(val client: S3Client) {

  private val Log = LoggerFactory.getLogger(this::class.java)

  fun run() {
    bucketExistsNoBuckets()
    listBucketsWithNoBuckets()
    testBucketCreation()
    testBucketExistsWithBucket()
    testBucketCreateWithConflict()
  }

  private fun bucketExistsNoBuckets() {
    Log.info("Testing bucket exists check with non-existent bucket.")

    val name = BucketName("no-bucket-here")

    try {
      assertNoBuckets()

      if (client.bucketExists(name)) {
        Log.error("Failed! S3Client.bucketExists returned true when false was expected.")
        return
      }
    } catch (e: Throwable) {
      Log.error("Failed with exception!", e)
      throw e
    }

    Log.info("Success!")
  }

  private fun listBucketsWithNoBuckets() {
    Log.info("Testing list buckets when no buckets exist.")

    try {
      if (client.listBuckets().isNotEmpty()) {
        Log.error("Failed! S3Client.listBuckets returned a non-zero number of buckets when zero buckets were expected.")
        return
      }
    } catch (e: Throwable) {
      Log.error("Failed with exception!", e)
      throw e
    }

    Log.info("Success!")
  }

  private fun testBucketCreation() {
    Log.info("Testing bucket creation.")

    val name = BucketName("foo")

    try {
      assertNoBuckets()

      Log.debug("Creating bucket.")
      client.createBucket(name)

      assertBucketExists(name)

      Log.info("Success!")
    } catch (e: Throwable) {
      Log.error("Failed with exception!", e)
      throw e
    } finally {
      cleanup(name)
    }
  }

  private fun testBucketExistsWithBucket() {
    Log.info("Testing bucket exists check when the target bucket is present.")

    val name = BucketName("bar")

    try {
      assertNoBuckets()

      Log.debug("Creating bucket.")
      client.createBucket(name)

      assertBucketExists(name)

      Log.info("Success!")
    } catch (e: Throwable) {
      Log.error("Failed with exception!", e)
      throw e
    } finally {
      cleanup(name)
    }
  }

  private fun testBucketCreateWithConflict() {
    Log.info("Testing bucket creation with a name conflict.")

    assertNoBuckets()

    val name = BucketName("hello-world")

    try {
      Log.debug("Creating bucket 1.")
      client.createBucket(name)

      Log.debug("Attempting to create bucket 2.")
      client.createBucket(name)

      Log.error("Failed!  S3Client.createBucket did not throw BucketAlreadyExistsException.")
    } catch (e: BucketAlreadyExistsException) {
      Log.info("Success!")
    } catch (e: Throwable) {
      Log.error("Failed with exception!", e)
      throw e
    } finally {
      cleanup(name)
    }
  }

  private fun testCreateIfNotExistsWithConflict() {
    Log.info("Testing bucket upsert with a name conflict.")

    assertNoBuckets()

    val name = BucketName("goodbye")

    try {
      Log.debug("Creating bucket 1.")
      client.createBucket(name)

      Log.debug("Attempting to create bucket 2.")
      client.createBucketIfNotExists(name)

      Log.info("Success!")
    } catch (e: Throwable) {
      Log.error("Failed with exception!", e)
      throw e
    } finally {
      cleanup(name)
    }
  }

  private fun testCreateIfNotExistsWithNoConflict() {
    Log.info("Testing bucket upsert with no name conflict.")

    val name = BucketName("nope")

    Log.debug("Assert bucket does not exist.")
    if (client.listBuckets().isNotEmpty()) {
      Log.error("Test precondition failed, bucket list is not empty.")
      return cleanup(name)
    }

    Log.debug("Upsert new bucket.")
    client.createBucketIfNotExists(name)

    Log.debug("Assert new bucket exists")
    if (!client.bucketExists(name)) {
      Log.error("Failed!")
      return cleanup(name)
    }

    Log.info("Success!")
    cleanup(name)
  }

  // test tag bucket
  // test remove bucket tags
  // test delete bucket
  // test create bucket with no conflict
  // test createIfNotExists with no conflict
  // test getBucket with no bucket
  // test getBucket with bucket
  // test listBuckets with a bucket

  private fun assertBucketExists(name: BucketName) {
    Log.debug("Ensuring bucket $name exists.")
    if (!client.bucketExists(name)) {
      Log.error("Test condition failed!  Bucket $name does not exist!")
      throw RuntimeException("Tests failed.")
    }
  }

  private fun assertNoBuckets() {
    Log.debug("Ensuring bucket list is empty.")

    if (client.listBuckets().isNotEmpty()) {
      Log.error("Test precondition failed!  Bucket list is not empty.")
      throw RuntimeException("Tests failed.")
    }
  }

  private fun cleanup(name: BucketName) {
    Log.debug("Performing test cleanup for bucket $name")
    client.deleteBucket(name)

    if (client.listBuckets().isEmpty()) {
      Log.error("Test cleanup failed! Bucket list is not empty.")
      throw RuntimeException("Tests failed.  Please cleanup minio image with 'docker-compose down'")
    }
  }
}