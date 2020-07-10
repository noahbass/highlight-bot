ARG VERSION=11

FROM openjdk:${VERSION}-jdk-slim as BUILD

COPY . /app
WORKDIR /app
RUN ./gradlew build

# CMD ["java", "-jar", "/app/build/libs/untitled-1.0-SNAPSHOT.jar"]


FROM openjdk:${VERSION}-jre

COPY --from=BUILD /app/build/libs/untitled-1.0-SNAPSHOT.jar /bin/runner/run.jar
WORKDIR /bin/runner

CMD ["java", "-jar", "run.jar"]
