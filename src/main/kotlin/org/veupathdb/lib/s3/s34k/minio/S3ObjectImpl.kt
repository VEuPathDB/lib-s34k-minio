package org.veupathdb.lib.s3.s34k.minio

import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.*
import org.veupathdb.lib.s3.s34k.params.ExistsParams
import org.veupathdb.lib.s3.s34k.params.StatParams
import org.veupathdb.lib.s3.s34k.params.TagGetParams
import org.veupathdb.lib.s3.s34k.params.TagPutParams
import org.veupathdb.lib.s3.s34k.params.`object`.ObjectTagGetParams
import org.veupathdb.lib.s3.s34k.params.`object`.ObjectTagPutParams

open class S3ObjectImpl(
  bucket:  S3Bucket,
  region:  String?,
  headers: S3Headers,
  path:    String
) : S3Object, S3ObjectResponseImpl(bucket, region, headers, path) {

  private val Log = LoggerFactory.getLogger(this::class.java)

  // region: Exists

  override fun exists(): Boolean {
    Log.trace("exists()")
    return bucket.objectExists(path)
  }

  override fun exists(action: ExistsParams.() -> Unit): Boolean {
    Log.trace("exists(action = {})", action)
    return bucket.objectExists(ExistsParams().also(action).toObjectExistsParams(path))
  }

  override fun exists(params: ExistsParams): Boolean {
    Log.trace("exists(params = {})", params)
    return bucket.objectExists(params.toObjectExistsParams(path))
  }

  // endregion


  // region: Stat

  override fun stat(): S3ObjectMeta {
    Log.trace("stat()")
    return bucket.statObject(path)
  }

  override fun stat(params: StatParams): S3ObjectMeta {
    Log.trace("stat(params = {})", params)
    return bucket.statObject(params.toObjectStatParams(path))
  }

  override fun stat(action: StatParams.() -> Unit): S3ObjectMeta {
    Log.trace("stat(action = {})", action)
    return bucket.statObject(StatParams().also(action).toObjectStatParams(path))
  }

  // endregion


  // region: Set Tags

  override fun setTag(key: String, value: String, cb: (() -> Unit)?) {
    Log.trace("setTag(key = {}, value = {})", key, value)

    bucket.putObjectTags {
      callback = cb
      path = this@S3ObjectImpl.path
      addTag(key, value)
    }
  }

  override fun setTags(vararg tags: S3Tag, cb: (() -> Unit)?) {
    Log.trace("setTags(tags = {})", tags)

    bucket.putObjectTags {
      callback = cb
      path = this@S3ObjectImpl.path
      addTags(*tags)
    }
  }

  override fun setTags(tags: Collection<S3Tag>, cb: (() -> Unit)?) {
    Log.trace("setTags(tags = {})", tags)

    bucket.putObjectTags {
      callback = cb
      path = this@S3ObjectImpl.path
      addTags(tags)
    }
  }

  override fun setTags(tags: Map<String, String>, cb: (() -> Unit)?) {
    Log.trace("setTags(tags = {})", tags)

    bucket.putObjectTags {
      callback = cb
      path = this@S3ObjectImpl.path
      addTags(tags)
    }
  }

  override fun setTags(params: TagPutParams, cb: (() -> Unit)?) {
    Log.trace("setTags(params = {}, cb = {})", params, cb)
    bucket.putObjectTags(params.toObjectTagPutParams(path, cb))
  }

  override fun setTags(action: TagPutParams.() -> Unit, cb: (() -> Unit)?) {
    Log.trace("setTags(action = {}, cb = {})", action, cb)
    bucket.putObjectTags(TagPutParams().also(action).toObjectTagPutParams(path, cb))
  }

  // endregion


  // region: Get Tags

  override fun getTags(cb: ((S3TagSet) -> Unit)?): S3TagSet {
    Log.trace("getTags()")
    return bucket.getObjectTags {
      callback = cb
      path     = this@S3ObjectImpl.path
    }
  }

  override fun getTags(params: TagGetParams, cb: ((S3TagSet) -> Unit)?): S3TagSet {
    Log.trace("getTags(params = {})", params)
    return bucket.getObjectTags(params.toObjectTagGetParams(path, cb))
  }

  override fun getTags(action: TagGetParams.() -> Unit, cb: ((S3TagSet) -> Unit)?): S3TagSet {
    Log.trace("getTags(action = {})", action)

    return bucket.getObjectTags(TagGetParams()
      .also(action)
      .toObjectTagGetParams(path, cb))
  }

  // endregion
}