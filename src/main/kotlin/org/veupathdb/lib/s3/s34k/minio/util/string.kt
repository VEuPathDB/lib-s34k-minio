package org.veupathdb.lib.s3.s34k.minio.util

internal inline fun String.isRoot() =
  startsWith('/')

internal inline fun String.trimRoot() =
  if (isRoot())
    trimStart('/')
  else
    this

internal inline fun String.hasDirPath() =
  contains('/')

internal inline fun String.firstSegment() =
  trimRoot().let { it.substring(0, indexOf('/')) }

internal inline fun String.addSlash() =
  if (endsWith('/'))
    this
  else
    "$this/"