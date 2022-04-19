import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
  id("org.jetbrains.kotlin.jvm") version "1.6.20"
  id("com.github.johnrengelman.shadow") version "7.1.2"
  application
}

repositories {
  mavenCentral()
  mavenLocal()

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

  implementation("org.veupathdb.lib.s3:s34k-minio:0.1.0-SNAPSHOT") { isChanging = true }

  implementation("org.apache.logging.log4j:log4j-slf4j18-impl:2.17.2")

  testImplementation(kotlin("test"))
}

application {
  mainClass.set("test.AppKt")
}

tasks.shadowJar {
  archiveBaseName.set("service")
  archiveClassifier.set("")
  archiveVersion.set("")

  exclude("**/Log4j2Plugins.dat")
}

tasks.test {

  testLogging {
    events.addAll(listOf(TestLogEvent.FAILED,
      TestLogEvent.SKIPPED,
      TestLogEvent.STANDARD_OUT,
      TestLogEvent.STANDARD_ERROR,
      TestLogEvent.PASSED))

    exceptionFormat = TestExceptionFormat.FULL
    showExceptions = true
    showCauses = true
    showStackTraces = true
    showStandardStreams = true
    enableAssertions = true
  }

  useJUnitPlatform()
}

application {
  mainClass.set("test.AppKt")
}
