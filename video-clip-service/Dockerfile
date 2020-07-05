ARG VERSION=11

FROM openjdk:${VERSION}-jdk-slim as BUILD

COPY . /src
WORKDIR /src
RUN ./gradlew build --refresh-dependencies

# RUN ls -ll
# RUN ls -ll ./src
# RUN ls -ll ./build/libs


FROM openjdk:${VERSION}-jre

COPY --from=BUILD /src/build/libs/untitled-1.0-SNAPSHOT.jar /bin/runner/run.jar
WORKDIR /bin/runner

CMD ["java", "-jar", "run.jar"]
