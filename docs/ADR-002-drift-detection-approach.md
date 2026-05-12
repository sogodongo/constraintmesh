# ADR-002: Drift detection approach

**Status:** Accepted  
**Date:** 2026-05-12

---

## Context

The risk management system (Article 9) needs to detect when a model's prediction score distribution shifts meaningfully from its established behaviour. There are two broad approaches: statistical process control and ML-based drift detection (KS test ensembles, MMD, etc.).

There's also an implementation question: whether to run this inside a proper Flink job or as a Kafka consumer loop in Python.

---

## Decisions

**Detection method:** Western Electric SPC rules applied to the prediction score stream.

**Runtime:** Python Kafka consumer loop, not a Flink job.

---

## Rationale

### SPC over ML-based detection

SPC has three properties that matter here:

First, it requires no model training. An ML-based drift detector needs a reference dataset and a retraining pipeline. That's a dependency on the very thing we're trying to observe. SPC needs only the first N observations to establish baseline control limits.

Second, it produces auditable, human-readable violations. "The score crossed 3 sigma above the baseline mean" is a statement a compliance officer can verify and include in technical documentation. "The MMD distance exceeded 0.12" is not.

Third, it runs at zero inference cost. The observability layer should be lighter than the system it observes. On a constrained node, an additional ML model running alongside the primary model is not a reasonable assumption.

The tradeoff is that SPC assumes roughly normal distributions. For classification models with bimodal score distributions, the false positive rate increases. The sigma thresholds are configurable as a partial mitigation. If multivariate drift detection becomes necessary, this decision should be revisited.

### Kafka consumer loop over Flink

Running a Flink cluster locally requires at minimum a JobManager and one TaskManager container, adds JVM memory pressure, requires Java 11+ alongside Python, and introduces a deployment gap: the local Flink version must match the production EMR version, which it frequently doesn't.

For this project's throughput (a few hundred spans per minute), a Python consumer loop with the same processing logic is functionally equivalent. The SPC calculation is stateful but not distributed - it only needs to maintain per-model windows, which fit easily in process memory.

When this moves to production at scale (>10k spans/minute), the consumer loop becomes the bottleneck and the migration to Flink becomes necessary. The processing logic in `drift_detector.py` is written to be stateless at the function level so that migration is straightforward.

---

## Consequences

- Local development is significantly simpler - no Flink containers, no JVM
- Production migration to Flink requires wrapping the SPC logic in a Flink ProcessFunction, which is a known and bounded effort
- SPC false positive rate on non-normal distributions is a known limitation, documented in the README
