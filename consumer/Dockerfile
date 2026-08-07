FROM eclipse-temurin:25-jdk AS build

WORKDIR /workspace

COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY producer/pom.xml producer/pom.xml
COPY consumer/pom.xml consumer/pom.xml
RUN ./mvnw -B -pl consumer -am dependency:go-offline

COPY consumer/src consumer/src
RUN ./mvnw -B -pl consumer -am -Dmaven.test.skip=true package

FROM eclipse-temurin:25-jre-alpine

RUN apk add --no-cache curl

WORKDIR /app
COPY --from=build /workspace/consumer/target/consumer-1.0.0-SNAPSHOT.jar app.jar

ENV TZ=UTC
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75"

USER 10001

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
