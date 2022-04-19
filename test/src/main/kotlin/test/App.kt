package test

import org.veupathdb.lib.s3.s34k.S3Api
import org.veupathdb.lib.s3.s34k.S3Config

fun main() {
  val client = S3Api.newClient(S3Config("minio", System.getenv("ACCESS_KEY"), System.getenv("ACCESS_TOKEN"), null))

  val bucketList = client.listBuckets()
}

