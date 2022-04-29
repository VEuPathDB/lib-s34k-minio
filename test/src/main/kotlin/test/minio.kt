package test

import io.minio.*
import io.minio.messages.DeleteObject
import org.slf4j.LoggerFactory

private val Log = LoggerFactory.getLogger("MinioKt")

internal fun MinioClient.makeBucket(name: String) {
  Log.trace("makeBucket(name = {})", name)
  makeBucket(MakeBucketArgs.builder().bucket(name).build())
}

internal fun MinioClient.clearBuckets() {
  Log.trace("clearBuckets()")

  Log.debug("Attempting to clear buckets from the minio store")

  Log.debug("- Cleanup: Listing buckets")
  val list = listBuckets()

  for (i in list) {
    Log.debug("- Cleanup: Listing objects in bucket '{}'", i.name())
    val objects = listObjects(ListObjectsArgs.builder()
      .bucket(i.name())
      .maxKeys(1000)
      .recursive(true)
      .build())

    val deletes = objects.map { DeleteObject(it.get().objectName()) }

    Log.debug("- Cleanup: {} objects found", deletes.size)

    Log.debug("- Cleanup: Deleting objects from bucket '{}'", i.name())
    val res = removeObjects(RemoveObjectsArgs.builder()
      .bucket(i.name())
      .objects(deletes)
      .build())

    for (j in res) {
      Log.error("- Cleanup: Failed to delete object {} with code {}", j.get().objectName(), j.get().code())
    }

    Log.debug("- Cleanup: Deleting bucket '{}'", i.name())
    removeBucket(RemoveBucketArgs.builder()
      .bucket(i.name())
      .build())
  }
}

internal fun MinioClient.makeObjects(bucket: String, vararg contents: Pair<String, String>) {
  Log.trace("makeObjects(bucket = {}, contents = {})", bucket, contents)

  for ((k, v) in contents) {
    Log.debug("Putting object {}", k)
    putObject(PutObjectArgs.builder()
      .bucket(bucket)
      .`object`(k)
      .stream(v.byteInputStream(), v.length.toLong(), -1)
      .build())
  }
}