@file:Suppress("NOTHING_TO_INLINE")

package org.veupathdb.lib.s3.s34k.minio

import org.veupathdb.lib.s3.s34k.core.requests.client.BasicS3BucketGetParams
import org.veupathdb.lib.s3.s34k.requests.client.S3BucketCreateParams


internal inline fun S3BucketCreateParams.toGetParams() =
  BasicS3BucketGetParams(bucketName, region, callback)
