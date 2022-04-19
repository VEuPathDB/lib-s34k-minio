package test

import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.S3Api
import org.veupathdb.lib.s3.s34k.S3Bucket
import org.veupathdb.lib.s3.s34k.S3Client
import org.veupathdb.lib.s3.s34k.S3Config

private val Log = LoggerFactory.getLogger("hello")

fun main() {
  Log.trace("main()")

  val client = S3Api.newClient(S3Config("http://minio", System.getenv("ACCESS_KEY"), System.getenv("ACCESS_TOKEN"), false, null))

  ClientTest(client).run()

  client.testEmptyBucketList()

  // delete bucket tags
  // put directory
  // delete empty directory
  // delete full directory
  // put object with input stream
  // put object tags
  // delete object tags
  // delete object
  //

}


/**
 * Retrieves the bucket list and requires that it is empty.
 */
private fun S3Client.testEmptyBucketList() {
  Log.info("Testing that the bucket list is empty")
  require(listBuckets().isEmpty())
  Log.info("Success!")
}

