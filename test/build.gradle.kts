plugins {
  id("org.jetbrains.kotlin.jvm") version "1.6.20"
  application
}

repositories {
  mavenCentral()
  mavenLocal()
}

dependencies {
  implementation(platform("org.jetbrains.kotlin:kotlin-bom"))
  implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

  implementation("")

  testImplementation(kotlin("test"))
}

tasks.test {
  useJUnitPlatform()
}

application {
  mainClass.set("test.AppKt")
}
