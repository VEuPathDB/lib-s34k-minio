package test

import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.S3Client

class ObjectTest(
  private val client: S3Client
) {

  private val Log = LoggerFactory.getLogger("ObjectTest")

  fun run(): Result {
    val out = Result()

    // TODO: delete when bucket does not exist
    // TODO: delete when object does not exist
    // TODO: delete when object exists

    // TODO: delete tags when bucket does not exist
    // TODO: delete tags when object does not exist

    // TODO: delete all tags when object exists

    // TODO: delete target tags when object has no tags
    // TODO: delete target tags when object has no overlapping tags
    // TODO: delete target tags when object has all overlapping tags
    // TODO: delete target tags when object has some overlapping tags

    // TODO: exists when bucket does not exist
    // TODO: exists when object does not exist
    // TODO: exists when object exists

    // TODO: stat when bucket does not exist
    // TODO: stat when object does not exist
    // TODO: stat when object exists

    // TODO: get tags when bucket does not exist
    // TODO: get tags when object does not exist
    // TODO: get tags when object exists

    // TODO: set tags when bucket does not exist
    // TODO: set tags when object does not exist
    // TODO: set tags when object exists

    return out
  }

}