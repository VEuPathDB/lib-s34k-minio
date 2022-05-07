import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
  kotlin("jvm") version "1.6.20"
  id("org.jetbrains.dokka") version "1.6.10"
  `java-library`
  `maven-publish`
}

group = "org.veupathdb.lib.s3"
version = "0.1.0-SNAPSHOT"

java {
  sourceCompatibility = JavaVersion.VERSION_1_8
  targetCompatibility = JavaVersion.VERSION_1_8

  withSourcesJar()
  withJavadocJar()
}

repositories {
  mavenLocal()
  mavenCentral()

  maven {
    name = "GitHubPackages"
    url  = uri("https://maven.pkg.github.com/veupathdb/packages")
    credentials {
      username = project.findProperty("gpr.user") as String? ?: System.getenv("GITHUB_USERNAME")
      password = project.findProperty("gpr.key") as String? ?: System.getenv("GITHUB_TOKEN")
    }
  }
}

dependencies {
  implementation(kotlin("stdlib"))
  implementation(kotlin("stdlib-jdk8"))

  api("org.veupathdb.lib.s3:s34k-core:0.3.0-SNAPSHOT") { isChanging = true }
  api("io.minio:minio:8.3.8")
  api("org.slf4j:slf4j-api:1.7.36")

  testImplementation(kotlin("test"))
}

tasks.dokkaHtml {
  outputDirectory.set(file("docs/dokka"))
}

tasks.dokkaJavadoc {
  outputDirectory.set(file("docs/javadoc"))
}

task("docs") {
  dependsOn("dokkaHtml", "dokkaJavadoc")
}

tasks.test {
  useJUnitPlatform()
}

tasks.withType<KotlinCompile>().all {
  kotlinOptions {
    jvmTarget = "1.8"
  }
}

publishing {
  repositories {
    maven {
      name = "GitHub"
      url = uri("https://maven.pkg.github.com/VEuPathDB/lib-s34k-minio")
      credentials {
        username = project.findProperty("gpr.user") as String? ?: System.getenv("USERNAME")
        password = project.findProperty("gpr.key") as String? ?: System.getenv("TOKEN")
      }
    }
  }

  publications {
    create<MavenPublication>("gpr") {
      from(components["java"])
      pom {
        name.set("Generalized S3 API")
        description.set("Provides a standard API for S3 operations which may be backed by varying implementations.")
        url.set("https://github.com/VEuPathDB/lib-s34k-minio")
        developers {
          developer {
            id.set("epharper")
            name.set("Elizabeth Paige Harper")
            email.set("epharper@upenn.edu")
            url.set("https://github.com/foxcapades")
            organization.set("VEuPathDB")
          }
        }
        scm {
          connection.set("scm:git:git://github.com/VEuPathDB/lib-s34k-minio.git")
          developerConnection.set("scm:git:ssh://github.com/VEuPathDB/lib-s34k-minio.git")
          url.set("https://github.com/VEuPathDB/lib-s34k-minio")
        }
      }
    }
  }
}
