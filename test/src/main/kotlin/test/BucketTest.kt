package test

import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.S3Bucket
import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.fields.BucketName
import java.util.Random

class BucketTest(private val client: S3Client) {

  private val Log = LoggerFactory.getLogger("BucketTest")

  private val chars = "abcdefghijklmnopqrstuvwxyz0123456789"

  fun run(): Result {
    val out = Result()

    //
    // Bucket operation tests.
    //

    Log.info("Bucket.delete when bucket exists")
    out.add(withBucket(this::deleteBucketWhenExists))
    Log.info("Bucket.delete when bucket does not exist")
    out.add(withBucket(this::deleteBucketWhenNotExists))
    // TODO: when bucket is not empty

    // TODO: delete recursive when bucket doesn't exist
    // TODO: delete recursive when bucket does exist and is empty
    // TODO: delete recursive when bucket does exist but is not empty

    // TODO: get bucket tags when bucket doesn't exist
    // TODO: get bucket tags when bucket does exist

    // TODO: put bucket tags when bucket doesn't exist
    // TODO: put bucket tags when bucket does exist

    // TODO: delete all bucket tags when bucket does not exist
    // TODO: delete all bucket tags when bucket does exist
    // TODO: delete target bucket tags on bucket with no tags
    // TODO: delete target bucket tags on bucket with no overlapping tags
    // TODO: delete target bucket tags on bucket with all tags targeted
    // TODO: delete target bucket tags on bucket with some overlap with existing tags

    //
    // Object operation tests
    //

    // TODO: object exists when bucket does not exist
    // TODO: object exists when object does not exist
    // TODO: object exists when object does exist

    // TODO: stat object when bucket does not exist
    // TODO: stat object when object does not exist
    // TODO: stat object when object does exist

    // TODO: get object when bucket does not exist
    // TODO: get object when object does not exist
    // TODO: get object when object does exist

    // TODO: put object when bucket does not exist
    // TODO: put object when object already exists (what is the desired behavior here)
    // TODO: put object when object does not already exist

    // TODO: get object tags when bucket does not exist
    // TODO: get object tags when object does not exist
    // TODO: get object tags when object does exist

    // TODO: put object tags when bucket does not exist
    // TODO: put object tags when object does not exist
    // TODO: put object tags when object does exist

    // TODO: delete all object tags when bucket does not exist
    // TODO: delete all object tags when object does not exist
    // TODO: delete all object tags when object does exist
    // TODO: delete target object tags on object with no tags
    // TODO: delete target object tags on object with no overlapping tags
    // TODO: delete target object tags on object with all tags targeted
    // TODO: delete target object tags on object with some overlap with existing tags

    // TODO: download object when bucket does not exist
    // TODO: download object when object does not exist
    // TODO: download object when object does exist

    // TODO: upload object when bucket does not exist
    // TODO: upload object when file does not exist
    // TODO: upload object when object already exists
    // TODO: upload object when object does not exist and local file does exist

    // TODO: touch object when bucket does not exist
    // TODO: touch object when object already exists
    // TODO: touch object when object does not exist

    // TODO: put directory when bucket does not exist
    // TODO: put directory when object already exists
    // TODO: put directory when object does not exist


    return out
  }

  // region Delete Bucket

  private fun deleteBucketWhenExists(bucket: S3Bucket): Boolean {
    Log.debug("Attempting to delete bucket '{}'.", bucket.bucketName)
    try {
      if (!bucket.delete())
        return Log.fail("expected delete() to return true but it returned false")
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    return Log.succeed()
  }

  private fun deleteBucketWhenNotExists(bucket: S3Bucket): Boolean {

    Log.debug("Setup: pre-deleting bucket.")
    try {
      client.deleteBucket(bucket.bucketName)
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    Log.debug("Attempting to delete bucket '{}'", bucket.bucketName)
    try {
      if (bucket.delete())
        return Log.fail("expected delete() to return false but it returned true")
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    return Log.succeed()
  }

  private fun deleteBucketWhenNotEmpty(bucket: S3Bucket): Boolean {
    Log.debug("Setup: Putting objects into the target bucket")
    
  }

  // endregion Delete Bucket

  // region Delete Bucket Recursive

  //

  // endregion Delete Bucket Recursive

  private inline fun withBucket(action: S3Bucket.() -> Boolean): Boolean {
    val name = BucketName(randomName())

    try {
      Log.debug("Creating bucket '{}'", name)
      return client.createBucket(name).action()
    } finally {
      Log.debug("Deleting bucket '{}'", name)

      if (client.bucketExists(name))
        client.deleteBucket(name)
    }
  }


  private fun randomName(): String {
    val out = StringBuilder(63)
    val ran = Random(System.currentTimeMillis())

    for (i in 0..62) {
      out.append(chars[ran.nextInt(36)])
    }

    return out.toString()
  }
}