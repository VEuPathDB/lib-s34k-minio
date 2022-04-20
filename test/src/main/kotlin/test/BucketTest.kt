package test

import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.S3Bucket
import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.params.bucket.BucketName
import java.util.Random

class BucketTest(private val client: S3Client) {

  private val Log = LoggerFactory.getLogger("BucketTest")

  private val chars = "abcdefghijklmnopqrstuvwxyz0123456789"

  fun run(): Result {
    val out = Result()

    // delete bucket when exists
    // delete bucket when no longer exists

    // object exists when exists
    // object exists when not exists

    // stat object when exists
    // stat object when not exists

    // get object when exists
    // get object when not exists

    // get object tags when exists
    // get object tags when not exists

    // put object tags when exists
    // put object tags when not exists

    // get bucket tags when exists
    // get bucket tags when not exists

    // put bucket tags when exists
    // put bucket tags when not exists

    // download object when exists
    // download object when not exists

    // touch object when exists
    // touch object when not exists

    // put directory when exists
    // put directory when not exists

    // delete bucket tag when exists
    // delete bucket tag when not exists

    return out
  }

  private inline fun withBucket(action: S3Bucket.() -> Boolean): Boolean {
    val name = BucketName(randomName())

    try {
      Log.debug("Creating bucket '$name'")
      val buck = client.createBucket(name)

      Log.debug("Asserting bucket exists")
      if (!client.bucketExists(name)) {
        Log.error("Test precondition failed: failed to create bucket '$name'")
        return false
      }

      return buck.action()
    } finally {
      Log.debug("Deleting bucket '$name'")
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