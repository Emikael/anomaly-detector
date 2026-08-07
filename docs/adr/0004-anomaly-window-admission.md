# ADR 0004: Anomaly window admission

## Context

A rolling Z-score reference can be contaminated by the very anomalies it is meant to find. Admitting an isolated extreme value inflates the next standard deviation and can mask a nearby outlier. Excluding every anomaly fixes that masking but has a worse failure mode: after a legitimate level shift, the old reference never accepts the new regime and alarms forever.

## Decision

With the default `DETECTOR_EXCLUDE_ANOMALIES=true`, an anomaly is scored but is not immediately admitted. The concrete `ZScoreDetector` keeps a FIFO pending buffer. A non-anomalous result clears a short pending run and enters normally. On the Kth consecutive anomaly, where default `DETECTOR_CONSECUTIVE_OVERRIDE=5`, the detector appends all K buffered values in arrival order after scoring the Kth point.

The level-shift producer mode exists to demonstrate this policy. Sequence 400 is produced at the old mean; sequence 401 begins the new mean. No new "regime shift" detection status is invented—the fifth point is still returned as `ANOMALY`.

## Consequences

- Separated point anomalies remain out of the reference window, reducing masking for the chosen point-outlier problem.
- Sustained shifts are admitted in K-sized groups and the finite window can re-baseline. The return to `OK` is an adaptation consequence, not proof that a shift was statistically identified.
- K=5 is a domain guess. It can admit a burst of unrelated anomalies or delay adaptation; labelled data or an explicit change-point detector is needed to tune or replace it.
- Pending values, the rolling window, and the policy state are in memory. Restarting the consumer starts cold and loses a partially buffered run.
