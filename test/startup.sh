#!/usr/bin/env sh

while ! nc -z minio 80; do
  echo "waiting for minio"
  sleep 1
done

java -jar /build/libs/service.jar