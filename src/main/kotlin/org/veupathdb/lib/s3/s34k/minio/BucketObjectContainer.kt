package org.veupathdb.lib.s3.s34k.minio

import io.minio.GetObjectArgs
import io.minio.ListObjectsArgs
import io.minio.MinioClient
import io.minio.PutObjectArgs
import io.minio.RemoveObjectsArgs
import io.minio.StatObjectArgs
import io.minio.Result
import io.minio.UploadObjectArgs
import io.minio.messages.DeleteObject
import io.minio.messages.Item
import org.slf4j.LoggerFactory
import org.veupathdb.lib.s3.s34k.*
import org.veupathdb.lib.s3.s34k.buckets.S3Bucket
import org.veupathdb.lib.s3.s34k.core.objects.AbstractObjectContainer
import org.veupathdb.lib.s3.s34k.core.objects.BasicObjectList
import org.veupathdb.lib.s3.s34k.core.objects.BasicObjectStream
import org.veupathdb.lib.s3.s34k.core.objects.BasicSubPathListing
import org.veupathdb.lib.s3.s34k.errors.MultiObjectDeleteError
import org.veupathdb.lib.s3.s34k.errors.ObjectDeleteError
import org.veupathdb.lib.s3.s34k.errors.S34KError
import org.veupathdb.lib.s3.s34k.minio.fields.MHeaders
import org.veupathdb.lib.s3.s34k.minio.operations.*
import org.veupathdb.lib.s3.s34k.minio.operations.DirectoryDeleter
import org.veupathdb.lib.s3.s34k.minio.operations.ObjectDelete
import org.veupathdb.lib.s3.s34k.minio.operations.ObjectExists
import org.veupathdb.lib.s3.s34k.minio.operations.StatObject
import org.veupathdb.lib.s3.s34k.minio.util.*
import org.veupathdb.lib.s3.s34k.objects.*
import org.veupathdb.lib.s3.s34k.params.DeleteParams
import org.veupathdb.lib.s3.s34k.params.`object`.*
import org.veupathdb.lib.s3.s34k.params.`object`.directory.DirectoryDeleteParams
import org.veupathdb.lib.s3.s34k.params.`object`.multi.MultiObjectDeleteParams
import org.veupathdb.lib.s3.s34k.params.`object`.touch.ObjectTouchParams
import java.time.OffsetDateTime
import kotlin.streams.toList

internal class BucketObjectContainer(
  private val bucket: S3Bucket,
  private val minio: MinioClient,
) : AbstractObjectContainer() {

  private val log = LoggerFactory.getLogger(this::class.java)

  override fun contains(path: String, params: ObjectExistsParams): Boolean {
    log.debug("Attempting to test whether {} contains object '{}'", bucket, path)
    return ObjectExists(bucket, path, params, minio)
  }

  override fun countAll(filter: (String) -> Boolean): UInt {
    log.debug("Attempting to count the objects matching the given filter in {}", bucket)
    try {
      return minio.listObjects(ListObjectsArgs.builder()
        .bucket(bucket)
        .region(bucket)
        .recursive(true)
        .build())
        .toStream()
        .map(Result<Item>::get)
        .map(Item::objectName)
        .filter(filter)
        .count()
        .toUInt()
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to count objects in $bucket" }
    }
  }

  override fun countAll(pathPrefix: String?): UInt {
    log.debug("Attempting to count all objects matching the given prefix in {}", bucket)

    try {
      return minio.listObjects(ListObjectsArgs.builder()
        .bucket(bucket)
        .region(bucket)
        .recursive(true)
        .optPrefix(pathPrefix)
        .build())
        .toStream()
        .count()
        .toUInt()
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to count all prefixed objects in $bucket" }
    }
  }

  override fun countSubDirs(parent: String?): UInt {
    log.debug("Attempting to count subdirectories in {} under parent path {}", bucket, parent ?: "")

    // Minio hides the data for this endpoint (it ignores the CommonPrefixes
    // fields), so we have to do something weird as a workaround.
    //
    // We will list all the objects in the bucket and split the results on the
    // first `/` instance to get the subdirectories.  We will exclude all paths
    // that do not contain a `/` character.
    try {
      val prefix     = parent?.trimRoot()?.addSlash()
      val prefixSize = prefix?.length?.inc()

      return minio.listObjects(ListObjectsArgs.builder()
        .bucket(bucket)
        .region(bucket)
        .optPrefix(prefix)
        .recursive(true)
        .build())
        // Get a stream off the iterable
        .toStream()
        // Unwrap the result
        .map(Result<Item>::get)
        // Get the name of the object
        .map(Item::objectName)
        // Trim off a '/' prefix if it exists
        .map(String::trimRoot)
        .filter(String::hasDirPath)
        // Pop the prefix off the object path
        .map { if (prefixSize != null) it.substring(prefixSize) else it }
        // Filter out object paths that do not contain a '/' character
        .filter(String::hasDirPath)
        // Pop off the first path segment
        .map(String::firstSegment)
        // uniq the list of path segments
        .distinct()
        // count the number of segments
        .count()
        // convert to uint
        .toUInt()
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to count subdirs for prefix '$parent' in $bucket}'" }
    }
  }

  override fun delete(path: String, params: DeleteParams) {
    log.debug("Attempting to delete object '{}' in {}", path, bucket)
    ObjectDelete(bucket, path, params, minio)
  }

  override fun deleteAll(params: MultiObjectDeleteParams) {
    log.debug("Attempting to perform a multi-object delete in {}", bucket)

    try {
      val res = minio.removeObjects(RemoveObjectsArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        // TODO: governance mode
        .objects(params.paths.stream().map(::DeleteObject).toIterable())
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())

      val writ = res.iterator()

      if (!writ.hasNext())
        return

      val errors = ArrayList<ObjectDeleteError>(10)

      writ.forEach {
        val item = it.get()

        // Ignore NoSuchKey as we wanted to delete them anyway
        if (it.get().code() != S3ErrorCode.NoSuchKey)
          errors.add(ObjectDeleteError(item.objectName(), item.message(), item.code()))
      }

      if (errors.isNotEmpty())
        throw MultiObjectDeleteError(bucket.name, errors)

      params.callback?.invoke()
    } catch (e: Throwable) {
      throw if (e !is S34KError)
        e.throwCorrect { "Failed to delete target objects in $bucket" }
      else
        e
    }
  }

  override fun download(path: String, params: ObjectDownloadParams): FileObject {
    log.debug("Attempting to download object '{}' from {} to local file '{}'", path, bucket, params.localFile)

    try {
      // Require the file to be set before we start making requests.
      params.reqFile()

      // We don't use Minio's download method as it does not return the response
      // headers, which we need.
      //
      // This means we need a response that includes the headers, and we'll have
      // to manually copy the stream to the target file ourselves.
      val res = minio.getObject(GetObjectArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        .`object`(path)
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())

      // Copy the stream to the file.
      res.pipeTo(params.localFile!!.outputStream())

      val out = MFileObject(
        res.`object`(),
        res.lastModified(),
        res.eTag(),
        res.size(),
        res.region(),
        MHeaders(res.headers()),
        bucket,
        minio,
        params.localFile!!
      )

      params.callback?.invoke(out)

      return out
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to download object '$path' from bucket $bucket to local file '${params.localFile}'" }
    }
  }

  override fun get(path: String, params: ObjectGetParams): S3Object? {
    log.debug("Attempting to get object '{}' from {}", path, bucket)

    val res = try {
      minio.statObject(StatObjectArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        .`object`(path)
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())
    } catch (e: Throwable) {
      if (e.isNoSuchKey()) {
        params.callback?.invoke(null)
        return null
      }

      e.throwCorrect { "Failed to get object '$path' from $bucket" }
    }
    val out = MObject(
      res.`object`(),
      res.lastModified().toOffsetDateTime(),
      res.etag(),
      res.size(),
      res.region(),
      MHeaders(res.headers()),
      bucket,
      minio
    )
    params.callback?.invoke(out)
    return out
  }

  override fun listAll(params: ObjectListAllParams): ObjectList {
    log.debug("Attempting to list all objects in {}", bucket)

    val out = try {
      BasicObjectList(
        minio.listObjects(ListObjectsArgs.builder()
          .bucket(bucket)
          .region(params, bucket)
          .recursive(true)
          .headers(params.headers)
          .queryParams(params.queryParams)
          .build())
          .toStream()
          .map(Result<Item>::get)
          .map { MObject(
            it.objectName(),
            it.lastModified().toOffsetDateTime(),
            it.etag(),
            it.size(),
            bucket.region,
            MHeaders(),
            bucket,
            minio
          ) }
          .toIterable())
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to fetch object list from $bucket" }
    }

    params.callback?.invoke(out)

    return out
  }

  override fun list(params: ObjectListParams): ObjectList {
    log.debug("Attempting to list objects in {}", bucket)

    return try {
      BasicObjectList(
        minio.listObjects(ListObjectsArgs.builder()
          .bucket(bucket)
          .region(params, bucket)
          .recursive(true)
          .optPrefix(params.prefix)
          .headers(params.headers)
          .queryParams(params.queryParams)
          .build())
          .toStream()
          .map(Result<Item>::get)
          .map { MObject(
            it.objectName(),
            it.lastModified().toOffsetDateTime(),
            it.etag(),
            it.size(),
            bucket.region,
            MHeaders(),
            bucket,
            minio
          ) }
          .toIterable())
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to fetch object list from $bucket" }
    }
  }

  override fun listSubPaths(params: SubPathListParams): SubPathListing {
    log.debug("Attempting to list sub-paths under prefix \"{}\" with delimiter \"{}\" in bucket \"{}\"", params.prefix, params.delimiter, bucket)

    return try {
      val prefixes = ArrayList<String>(100)
      val objects  = minio.listObjects(ListObjectsArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        .prefix(params.prefix)
        .delimiter(params.delimiter)
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())
        .toStream()
        .map(Result<Item>::get)
        .peek { if (it.isDir) prefixes.add(it.objectName()) }
        .filter { !it.isDir }
        .map { MObject(
          it.objectName(),
          it.lastModified().toOffsetDateTime(),
          it.etag(),
          it.size(),
          bucket.region,
          MHeaders(),
          bucket,
          minio
        ) }
        .toIterable()

      // NOTE!!!!
      //
      // This only works because basic object list consumes the stream
      // immediately, if that was not the case, the prefixes list would be
      // empty!
      BasicSubPathListing(BasicObjectList(objects), prefixes)
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to fetch sub-path list from bucket $bucket" }
    }
  }

  override fun open(path: String, params: ObjectOpenParams): StreamObject? {
    log.debug("Attempting to open object '{}' in {}", path, bucket)

    val res = try {
      minio.getObject(GetObjectArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        .`object`(path)
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())
    } catch (e: Throwable) {
      if (e.isNoSuchKey()) {
        params.callback?.invoke(null)
        return null
      }

      e.throwCorrect { "Failed to get object '$path' from $bucket" }
    }

    val out = MStreamObject(
      res.`object`(),
      res.lastModified(),
      res.eTag(),
      res.size(),
      res.region(),
      res,
      MHeaders(res.headers()),
      bucket,
      minio
    )

    params.callback?.invoke(out)

    return out
  }

  override fun put(path: String, params: StreamingObjectPutParams): S3Object {
    log.debug("Attempting to put object '{}' into {} from a given stream.", path, bucket)

    try {
      val res = minio.putObject(PutObjectArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        .`object`(path)
        .stream(params.reqStream(), -1, params.partSize.toLong())
        .optContentType(params.contentType)
        .tags(params.tags.toMap())
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())

      val out = MObject(
        path,
        OffsetDateTime.now(),
        res.etag(),
        0L,
        res.region(),
        MHeaders(res.headers()),
        bucket,
        minio
      )

      params.callback?.invoke(out)

      return out
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to put object '$path' into $bucket" }
    }
  }

  override fun rmdir(path: String, params: DirectoryDeleteParams) {
    log.debug("Attempting to remove directory '{}' from {}", path, bucket)
    DirectoryDeleter(bucket, path, params, minio).execute()
  }

  override fun stat(path: String, params: ObjectStatParams): ObjectMeta? {
    log.debug("Attempting to stat object '{}' in {}", path, bucket)
    return StatObject(bucket, path, params, minio)
  }

  override fun stream(params: ObjectStreamParams): ObjectStream {
    return BasicObjectStream(
      minio.listObjects(ListObjectsArgs.builder()
        .bucket(bucket)
        .prefix(params.prefix)
        .region(params, bucket)
        .recursive(true)
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())
        .toStream()
        .map(Result<Item>::get)
        .map { MObject(
          it.objectName(),
          it.lastModified().toOffsetDateTime(),
          it.etag(),
          it.size(),
          bucket.region,
          MHeaders(),
          bucket,
          minio
        ) }
    )
  }

  override fun streamAll(params: ObjectStreamAllParams): ObjectStream {
    return BasicObjectStream(
      minio.listObjects(ListObjectsArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        .recursive(true)
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())
        .toStream()
        .map(Result<Item>::get)
        .map { MObject(
          it.objectName(),
          it.lastModified().toOffsetDateTime(),
          it.etag(),
          it.size(),
          bucket.region,
          MHeaders(),
          bucket,
          minio
        ) }
    )
  }

  override fun touch(path: String, params: ObjectTouchParams): S3Object {
    log.debug("Attempting to touch object '{}' in {}", path, bucket)
    return ObjectToucher(bucket, path, params, minio).execute()
  }

  override fun upload(path: String, params: FileUploadParams): S3Object {
    log.debug("Attempting to upload file '{}' to path '{}' in {}", params.localFile, path, bucket)
    try {
      val res = minio.uploadObject(UploadObjectArgs.builder()
        .bucket(bucket)
        .region(params, bucket)
        .`object`(path)
        .optContentType(params.contentType)
        .filename(params.reqFile().absolutePath)
        .tags(params.tags.toMap())
        .headers(params.headers)
        .queryParams(params.queryParams)
        .build())

      val out = MObject(
        res.`object`(),
        null,
        res.etag(),
        0L,
        res.region(),
        MHeaders(res.headers()),
        bucket,
        minio
      )

      params.callback?.invoke(out)

      return out
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to upload ${params.localFile} to path $path in $bucket" }
    }
  }

  override fun <R> withObject(path: String, action: S3Object.() -> R): R {
    try {
      val stat = minio.statObject(StatObjectArgs.builder()
        .bucket(bucket)
        .region(bucket)
        .`object`(path)
        .build())

      return MObject(
        path,
        stat.lastModified().toOffsetDateTime(),
        stat.etag(),
        stat.size(),
        bucket.region,
        MHeaders(stat.headers()),
        bucket,
        minio
      ).action()
    } catch (e: Throwable) {
      e.throwCorrect { "Failed to stat object '$path' in bucket '${bucket.name}'" }
    }
  }
}