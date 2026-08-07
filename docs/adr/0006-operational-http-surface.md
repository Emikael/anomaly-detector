# ADR 0006: Operational HTTP surface

## Context

A queue consumer still needs a concrete readiness signal, inspectable health, and scrapeable metrics. Log parsing is not a reliable readiness protocol, while exposing arbitrary Actuator endpoints can disclose environment values, configuration, and memory contents. The project must not grow a business API merely to make a demo observable.

## Decision

Embed Spring Boot's web server only for Actuator. The explicit web exposure allowlist is `health`, `info`, and `prometheus`; no controller accepts business input. The default consumer and producer ports are 8080 and 8081.

Actuator probes are enabled. Rabbit connectivity is included in the readiness group with `readinessState`; liveness contains only `livenessState`. The smoke test polls readiness endpoints and asserts `/actuator/env` is unavailable instead of trusting logs or a wildcard exposure setting.

## Consequences

- `/actuator/health`, `/actuator/info`, and `/actuator/prometheus` provide a narrow operational surface. The Prometheus endpoint exports the detector counters, gauge, and processing timer without a dashboard container.
- A Rabbit outage makes a service unready while liveness remains up, allowing the client/container to recover without a broker-driven restart storm.
- The web server and Actuator add image/runtime footprint to a queue worker, but make compose health checks and reviewer verification real rather than asserted.
- HTTP is limited to operations; Kubernetes manifests and HTTP business APIs remain out of scope.
