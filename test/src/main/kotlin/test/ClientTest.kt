package test

import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.params.bucket.BucketName

class ClientTest(val client: S3Client) {

  private val Log = LoggerFactory.getLogger(this::class.java)

  fun run() {
    bucketExistsNoBuckets()
  }

  private fun bucketExistsNoBuckets() {
    Log.info("Testing bucket exists check with non-existent bucket.")
    require(!client.bucketExists(BucketName("no-bucket-here")))
    Log.info("Success!")
  }

  private fun listBucketsWithNoBuckets() {
    Log.info("Testing list buckets when no buckets exist.")
    require(client.listBuckets().isEmpty())
    Log.info("Success!")
  }

  private fun testBucketCreation() {
    Log.info("Testing bucket creation.")

    Log.debug("Creating bucket.")
    client.createBucket(BucketName("foo"))

    Log.debug("Test bucket exists.")
    require(client.bucketExists(BucketName("foo")))

    Log.debug("Cleanup.")
    client.deleteBucket(BucketName("foo"))

    Log.info("Success!")
  }

  private fun testBucketExistsWithBucket() {
    Log.info("Testing bucket exists check when the target bucket is present.")

    Log.debug("Creating bucket.")
    client.createBucket(BucketName("bar"))

    Log.debug("Test Bucket Exists.")
    require(client.bucketExists(BucketName("bar")))

    Log.debug("Cleanup.")
    client.deleteBucket(BucketName("bar"))

    Log.info("Success")
  }

  // test create bucket with conflict
  // test createIfNotExists with conflict
  // test tag bucket
  // test remove bucket tags
  // test delete bucket
  // test create bucket with no conflict
  // test createIfNotExists with no conflict
  // test getBucket with no bucket
  // test getBucket with bucket
  // test listBuckets with a bucket

}