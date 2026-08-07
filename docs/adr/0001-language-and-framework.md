# ADR 0001: Language and framework

## Context

The exercise permits Java or Python, but the system needs a reproducible JVM toolchain, AMQP lifecycle support, and an operational surface without turning the project into a business HTTP service. Earlier planning prose mentioned Java 21 and a possible Boot fallback; that no longer describes the implemented release.

## Decision

Use Java 25 with the Maven Wrapper and Spring Boot 4.1.0. Both services use Spring AMQP for broker lifecycle and delivery integration, Actuator plus Prometheus for operations, and the web starter only to expose that operational surface. There are no business controllers, persistence layer, JPA dependency, security subsystem, or shared producer/consumer application module.

The statistical core remains outside Spring in the framework-free `consumer.detection` package. Spring AMQP 4 and Jackson 3 compatibility is covered by a real-broker integration test rather than assumed from older Boot conventions.

## Consequences

- Java 25 is the selected current LTS, not a claim that production source requires Java-25-only language features. Records and the ordinary Java used here would be viable on earlier releases; the selected runtime is a maintenance and toolchain choice.
- Spring Boot 4.1 brings Spring Framework 7, Spring AMQP 4, and Jackson 3 behavior that must be kept in the test matrix. The repository does not silently fall back to Boot 3.x.
- The application carries a servlet container and Actuator dependency in exchange for real health/readiness and Prometheus endpoints. That is operational overhead the smallest queue consumer would not otherwise need.
- Maven, Docker images, and CI must provide Java 25. PIT was retained after the current toolchain proved it can mutate the detection package.
