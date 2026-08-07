# ADR 0005: Single-threaded consumption

## Context

The detector's rolling window, pending anomaly buffer, recent-ID cache, and sequence tracker are mutable and order-dependent. Concurrent delivery from one queue would make the values admitted to the window nondeterministic. Ordering messages after receipt would add latency and state for a condition the selected broker topology is designed to avoid.

## Decision

Run exactly one Rabbit listener consumer with `concurrency=1`, `prefetch=1`, and manual acknowledgement. The listener validates and processes in arrival order, then acknowledges only after successful processing. Duplicate IDs are acknowledged and skipped before statistical state changes. Sequence gaps and out-of-order values are logged as warnings but are still evaluated in arrival order; the implementation neither buffers nor reorders them.

The detector and cache are intentionally non-thread-safe under this one-listener invariant. The cache capacity is ten times the configured window size and is in memory only.

## Consequences

- Per-queue throughput is bounded by one in-flight delivery, which favors deterministic state and a clear crash/redelivery story over maximum parallelism.
- The delivery path is at-least-once. Remembering a successful ID before ack reduces redelivery double counting, but a process crash cannot provide exactly-once behavior or survive a restart without persistence.
- Scale-out must partition by metric key: one ordered state owner per RabbitMQ consistent-hash route, Kinesis shard, or Kafka partition. Adding consumers to this single queue is not a safe scaling strategy.
- The selected design exposes an explicit weakness: its no-reorder assumption becomes less safe if producers or partitions change. That trade-off is documented instead of hidden by speculative buffering.
