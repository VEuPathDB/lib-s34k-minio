@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import org.veupathdb.lib.s3.s34k.params.bucket.BucketGetParams
import org.veupathdb.lib.s3.s34k.params.bucket.BucketPutParams


internal inline fun BucketPutParams.toGetParams() =
  BucketGetParams(bucket?.name, region, callback)