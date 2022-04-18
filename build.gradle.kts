import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
  kotlin("jvm") version "1.6.20"
}

group = "org.veupathdb.lib.s3"
version = "0.1.0-SNAPSHOT"

repositories {
  mavenCentral()
  mavenLocal()
}

dependencies {
  implementation(kotlin("stdlib"))
  implementation(kotlin("stdlib-jdk8"))

  api("org.veupathdb.lib.s3:s34k:0.1.0-SNAPSHOT") { isChanging = true}
  api("io.minio:minio:8.3.8")
  api("org.slf4j:slf4j-api:1.7.36")

  testImplementation(kotlin("test"))
}

tasks.test {
  useJUnitPlatform()
}

tasks.withType<KotlinCompile>().all {
  kotlinOptions {
    jvmTarget = "1.8"
  }
}
