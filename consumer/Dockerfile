FROM eclipse-temurin:25-jdk AS build

WORKDIR /workspace

COPY .mvn .mvn
# lombok.config is a build input, not a convenience: it pins the @Slf4j field name this code calls,
# so omitting it does not fall back to a default — it fails the compile with "cannot find symbol".
COPY mvnw pom.xml lombok.config ./
COPY producer/pom.xml producer/pom.xml
COPY consumer/pom.xml consumer/pom.xml
RUN ./mvnw -B -pl consumer -am dependency:go-offline

COPY consumer/src consumer/src
RUN ./mvnw -B -pl consumer -am -Dmaven.test.skip=true package

FROM eclipse-temurin:25-jre-alpine

RUN apk add --no-cache curl

WORKDIR /app
COPY --from=build /workspace/consumer/target/consumer-1.0.0-SNAPSHOT.jar app.jar

# TZ pins the JVM to UTC so R11 timestamps carry Z regardless of the host.
ENV TZ=UTC

USER 10001

EXPOSE 8080

# MaxRAMPercentage goes on the command line, not JAVA_TOOL_OPTIONS: the latter makes every JVM start
# print "Picked up JAVA_TOOL_OPTIONS: ..." to stderr, which would be the first line of `docker compose up`.
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
