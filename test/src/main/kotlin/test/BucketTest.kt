package test

import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.buckets.S3Bucket
import org.veupathdb.lib.s3.s34k.errors.BucketNotEmptyError

class BucketTest(private val client: S3Client) {

  private val Log = LoggerFactory.getLogger("BucketTest")

  fun run(): Result {
    val out = Result()

    //
    // Bucket operation tests.
    //

    Log.info("Bucket.delete when bucket exists")
    out.add(client.withBucket(Log, this::deleteBucketWhenExists))
    Log.info("Bucket.delete when bucket does not exist")
    out.add(client.withBucket(Log, this::deleteBucketWhenNotExists))
    Log.info("Bucket.delete when bucket is not empty")
    out.add(client.withBucket(Log, this::deleteBucketWhenNotEmpty))

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

    // TODO: delete directory when bucket does not exist
    // TODO: delete directory when object does not exist
    // TODO: delete directory when object has no sub-keys
    // TODO: delete directory when object has sub-keys


    return out
  }

  // region Delete Bucket

  private fun deleteBucketWhenExists(bucket: S3Bucket): Boolean {
    Log.debug("Attempting to delete bucket '{}'.", bucket.name)
    try {
      bucket.delete()
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    Log.debug("Verifying bucket was deleted.")
    try {
      if (client.buckets.exists(bucket.name))
        return Log.fail("Bucket was not deleted when it should have been")
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    return Log.succeed()
  }

  private fun deleteBucketWhenNotExists(bucket: S3Bucket): Boolean {

    Log.debug("Setup: pre-deleting bucket.")
    try {
      client.buckets.delete(bucket.name)
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    Log.debug("Attempting to delete bucket '{}'", bucket.name)
    try {
      bucket.delete()
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    return Log.succeed()
  }

  private fun deleteBucketWhenNotEmpty(bucket: S3Bucket): Boolean {
    Log.debug("Setup: Putting objects into the target bucket")
    try {
      bucket.objects.put("test/object/1.txt", "hello".byteInputStream())
    } catch (e: Throwable) {
      return Log.fail(e)
    }

    Log.debug("Attempting to delete non-empty bucket")
    return try {
      bucket.delete()
      Log.fail("Expected Bucket.delete to throw a BucketNotEmptyException, but no exception was thrown.")
    } catch (e: BucketNotEmptyError) {
      Log.succeed()
    } catch (e: Throwable) {
      Log.fail(e)
    }
  }

  // endregion Delete Bucket

  // region Delete Bucket Recursive

  //

  // endregion Delete Bucket Recursive
}