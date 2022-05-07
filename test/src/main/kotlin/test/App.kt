package test

import org.slf4j.LoggerFactory

private val Log = LoggerFactory.getLogger("AppKt")

fun main() {
  val minio  = MinioClient.builder()
    .endpoint("minio", 80, false)
    .credentials(System.getenv("ACCESS_KEY"), System.getenv("ACCESS_TOKEN"))
    .build()
  val client = S3Api.newClient(S3Config("minio", 80u, false, System.getenv("ACCESS_KEY"), System.getenv("ACCESS_TOKEN")))

  val result = Result()

  result += ClientTest(minio, client).run()
  result += BucketTest(client).run()

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
