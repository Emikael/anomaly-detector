# ADR 0002: Messaging system

## Context

The producer and consumer must survive late startup, expose delivery failure clearly, and support a reviewer-visible local demonstration. A fire-and-forget channel would lose the properties the consumer needs: durable buffering, acknowledgement, redelivery, and poison-message handling.

Redis Pub/Sub was rejected because it has no durable queue or native acknowledgement. LocalStack SQS and Floci were considered as AWS-shaped alternatives; Floci is lighter, but it is an emulator with a much younger operational track record. The local requirement is reliable first-run behavior and visible queue state, not an AWS API simulation.

## Decision

Use `rabbitmq:3.13-management-alpine` with explicit demo credentials and this durable topology:

```text
metrics.exchange (direct) -- metrics.datapoint --> metrics.datapoint.q
metrics.datapoint.q -- dead letter --> metrics.dlx --> metrics.datapoint.dlq
```

Both applications declare the same exchange, binding, durable main queue, durable DLQ, and queue arguments. The main queue has `x-max-length=10000` and `x-overflow=reject-publish`, so overload is visible rather than silently dropping the head of the queue. Producers publish persistent messages with correlated confirms and mandatory returns. The consumer uses one manual-ack listener with prefetch one; invalid input is rejected without requeue and unexpected failure follows bounded retry then confirmed DLQ republish.

## Consequences

- RabbitMQ makes acknowledgements, redelivery, DLQ messages, and queue depth inspectable at the local management UI. A consumer can start after the producer without losing accepted messages.
- `reject-publish` deliberately turns overload into a publisher failure/nack to investigate. It is not a hidden loss policy or an auto-scaling substitute.
- The result is at-least-once, not exactly-once: IDs are deduplicated in a bounded in-memory cache and all delivery state is lost on restart.
- A real production deployment may choose SQS, Kinesis, or Kafka, but must map ordering to FIFO message groups/shards/partitions and reproduce each platform's acknowledgement and dead-letter semantics. This repository does not add a second broker or a Kafka runtime.
