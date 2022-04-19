FROM alpine:3.15.4

ARG GITHUB_USERNAME
ARG GITHUB_TOKEN

RUN apk add --no-cache openjdk17-jdk

COPY ["settings.gradle.kts", "build.gradle.kts", "gradlew", "./"]
COPY gradle gradle/
COPY src src/

RUN ./gradlew shadowJar

CMD java -jar build/libs/service.jar


