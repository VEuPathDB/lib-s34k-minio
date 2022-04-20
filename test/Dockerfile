FROM alpine:3.15.4

ARG GITHUB_USERNAME
ARG GITHUB_TOKEN

RUN apk -U add --no-cache openjdk17-jdk

ENV PATH "$PATH:/gradle-${GRADLE_VERSION}/bin/"

COPY ["settings.gradle.kts", "build.gradle.kts", "gradlew", "startup.sh", "./"]
COPY gradle gradle/
COPY src src/

RUN ./gradlew shadowJar

CMD ./startup.sh


