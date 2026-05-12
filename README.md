# ConstraintMesh

A data governance layer for high-risk AI systems, built around the technical obligations in the EU AI Act.

The core problem: teams deploying AI in health, credit, and public services now have legal obligations around logging, data quality, drift monitoring, and human oversight. Most have no tooling that addresses these specifically. This project is that tooling, designed to run on the kind of infrastructure these teams actually have access to.

---

## What it does

- Instruments model inference endpoints and emits structured audit events (Article 12)
- Monitors prediction score distributions for drift using statistical process control (Article 9)
- Validates incoming feature distributions against training data profiles (Article 10)
- Stores the full audit trail in an Iceberg lakehouse with configurable retention (Article 26)
- Surfaces governance alerts and requires human acknowledgement before clearing (Article 14)
- Generates compliance evidence reports from the audit trail (Article 11)

The adaptive sampler reduces telemetry volume when uplink bandwidth is constrained, without dropping the audit trail. This matters for deployments that don't have reliable high-bandwidth connectivity.

---

## Architecture

```
edge node
  model inference
  -> OTLP agent (audit logger)
  -> adaptive sampler
  -> gRPC relay

ingestion
  OTel Collector
  -> Redpanda / MSK (otel.spans)

stream processing
  Flink
  -> SPC drift detector  -> alerts.drift
  -> data quality checks -> alerts.data_quality
  -> windowed metrics    -> metrics.windows

storage
  Iceberg on S3 / MinIO
  raw spans -> curated metrics -> compliance aggregates
  dbt metric layer

ai layer
  LangGraph agent
  -> queries Iceberg on alert
  -> generates incident summary via Claude API

presentation
  FastAPI
  React dashboard (oversight + alert acknowledgement)
  Airflow -> compliance report (PDF/markdown)
```

---

## Local setup

Requirements: Docker Desktop, Python 3.11+

```bash
git clone https://github.com/sogodongo/constraintmesh.git
cd constraintmesh

cp .env.example .env
# fill in ANTHROPIC_API_KEY

docker compose up -d
docker exec redpanda rpk topic create otel.spans metrics.windows \
  alerts.drift alerts.data_quality governance.oversight_actions \
  --brokers localhost:9092 --partitions 3 --replicas 1

pip install -r requirements.txt
python agents/collector.py
```

Verify spans are arriving:
```bash
docker exec redpanda rpk topic consume otel.spans --num 3
```

---

## Tests

```bash
pytest tests/ -v
```

Covers Article 12 attribute compliance, adaptive sampler behaviour at each bandwidth threshold, and drift injection correctness. Tests run without Docker.

---

## Cost

| Component | Local | AWS |
|-----------|-------|-----|
| Broker | Redpanda (free) | MSK kafka.t3.small x2 ~$90/mo |
| Storage | MinIO (free) | S3 + Iceberg ~$10/mo |
| Compute | Docker (free) | ECS Fargate ~$30/mo |
| Streaming | Local Flink (free) | EMR / self-managed ~$40/mo |

Rough total on AWS: ~$170/mo at small scale.

---

## Known issues

- The adaptive sampler uses a byte-budget estimate rather than actual measured bytes. Under large span payloads it will under-sample. A token-bucket approach would be more accurate.
- SPC detection assumes roughly normal score distributions. Heavily skewed models will produce more false positives. The sigma threshold is configurable as a partial mitigation.
- mTLS cert rotation is manual in this version.
- The compliance report is internal evidence, not a formal conformity assessment. Third-party audit is still required for Annex VII.

---

## Architecture decisions

| ADR | Topic |
|-----|-------|
| [ADR-001](docs/ADR-001-broker-redpanda-vs-msk.md) | Redpanda locally, MSK in production |
| [ADR-002](docs/ADR-002-drift-detection-approach.md) | SPC over ML-based drift detection |
| [ADR-003](docs/ADR-003-iceberg-vs-delta.md) | Iceberg over Delta Lake |
| [ADR-004](docs/ADR-004-dashboard-streaming.md) | SSE over WebSockets |

---

## EU AI Act coverage

| Article | Obligation | Component |
|---------|-----------|-----------|
| 9 | Risk management | Flink SPC detector |
| 10 | Data governance | Quality check pipeline |
| 11 | Technical documentation | Compliance report generator |
| 12 | Automatic logging | OTLP agent + Iceberg audit trail |
| 14 | Human oversight | React dashboard + acknowledgement log |
| 26 | Log retention (6 months min) | Iceberg retention policy |
