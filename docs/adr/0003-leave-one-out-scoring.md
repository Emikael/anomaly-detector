# ADR 0003: Leave-one-out scoring

## Context

The required Z-score formula does not say whether the arriving point belongs in the reference window used to score it. Including it makes a point inflate both the mean and standard deviation that judge it. For a window of size $n$, the maximum self-including Z-score is $(n - 1) / \sqrt{n}$, about 6.93 at the default window of 50.

The detector also runs under a small, bounded window where incremental deletion from running sums is not worth the numerical and reasoning risk. The release intentionally ships one concrete detector, not an unused abstraction for possible algorithms.

## Decision

Use the concrete, framework-free `ZScoreDetector` to score each value against the existing window, then mutate admission state. It computes the mean and sample standard deviation in two O(N) passes, using Bessel's $n - 1$ divisor, and declares an anomaly only when `z > threshold`.

The default window is 50, the warm-up floor is 30, and a standard deviation below `1e-12` produces `DEGENERATE_WINDOW` instead of a numeric verdict. The detector is stateful and documented for serialized use only; it has no Spring, AMQP, logging, metrics, or clock dependency.

## Consequences

- Extreme current points remain detectable rather than being bounded by their own inclusion. The golden tests cover the exact sample-sigma and leave-one-out behavior.
- With a sample estimate and leave-one-out scoring, the null behavior is t-like rather than exactly standard normal. Threshold 3.0 is therefore a practical configuration choice, not a calibrated false-positive guarantee.
- O(N) recomputation is trivial at N<=100 and avoids cancellation-prone running variance deletion. Any future optimization needs a benchmark and numerical regression evidence.
- The framework-free package boundary is the future extraction seam if another detector is justified. No `AnomalyDetector` interface claims pluggability that the release does not provide.
