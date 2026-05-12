# ADR-001: Kafka broker selection

**Status:** Accepted  
**Date:** 2026-05-12

---

## Context

The ingestion layer needs a durable message broker. Two environments need to work: local development on WSL2 with constrained resources, and AWS in production.

The requirements pull in different directions. Locally, startup time and memory footprint matter. In production, managed operations, IAM auth, and multi-AZ durability matter. The constraint is that producer/consumer code should not change between environments.

---

## Decision

Redpanda locally, Amazon MSK (Kafka 3.5) in production.

Both expose the same Kafka API. The only difference between environments is the `KAFKA_BOOTSTRAP` env var:

```
KAFKA_BOOTSTRAP=localhost:9092       # local
KAFKA_BOOTSTRAP=b-1.xxx.kafka.amazonaws.com:9092  # production
```

---

## Rationale

Redpanda runs as a single container with no JVM dependency. On WSL2 with 8 GB RAM shared between Windows and Linux, a full Kafka stack (broker + ZooKeeper or KRaft) is too heavy to run alongside Flink, MinIO, and the OTel collector. Redpanda starts in under 5 seconds and uses roughly 300 MB at rest.

MSK in production gives managed broker operations, CloudWatch metrics integration, and encryption in transit by default. The `kafka.t3.small` tier at two brokers covers the expected throughput for this use case at around $90/month.

Confluent Cloud was considered but rejected. It introduces a third-party data processor, which complicates the data residency posture for health AI inference records. Keeping data within the AWS account boundary is simpler to document for Article 26 compliance.

---

## Tradeoffs

Redpanda has subtle behavioural differences from Kafka in some edge cases, particularly around consumer group rebalancing at high partition counts. These are unlikely to surface at current scale but mean integration tests against Redpanda are not a full substitute for testing against MSK before a production rollout.

The `advertise-kafka-addr` in the Redpanda container config must be set to the container name (`redpanda`), not `localhost`. If set to `localhost`, other containers in the same Docker network resolve the broker address correctly on the initial connection but subsequent internal connections fail. This cost an hour to diagnose.

---

## Alternatives considered

| Option | Reason not chosen |
|--------|------------------|
| Apache Kafka locally | JVM overhead, slow startup on WSL2 |
| Confluent Cloud | Third-party data processor, residency complexity |
| Amazon Kinesis | No Kafka API compatibility, no local emulator |
| NATS JetStream | Smaller ecosystem, less operational familiarity |
