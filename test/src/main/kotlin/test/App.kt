package test

import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.S3Api
import org.veupathdb.lib.s3.s34k.S3Config

private val Log = LoggerFactory.getLogger("AppKt")

fun main() {
  val client = S3Api.newClient(S3Config("http://minio", System.getenv("ACCESS_KEY"), System.getenv("ACCESS_TOKEN"), false, null))

  val result = Result()

  result += ClientTest(client).run()

  // delete bucket tags
  // put directory
  // delete empty directory
  // delete full directory
  // put object with input stream
  // put object tags
  // delete object tags
  // delete object
  //

  Log.info("Successes: {}", result.successes)
  if (result.fails > 0) {
    Log.warn("Failures: {}", result.fails)
  } else {
    Log.info("Failures: {}", result.fails)
  }
}
