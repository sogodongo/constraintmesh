# ADR-003: Iceberg over Delta Lake for the audit trail

**Status:** Accepted
**Date:** 2026-05-12

---

## Context

The audit trail needs a table format that supports:
- Schema evolution (telemetry schemas change across agent versions)
- Time-travel queries (compliance queries need point-in-time reads)
- Partition evolution (initial partitioning by date may need adjustment)
- Reads from a lightweight Python process (the RCA agent and compliance report generator)
- Writes from a Kafka consumer process without a JVM

Both Apache Iceberg and Delta Lake satisfy the core requirements. The decision
comes down to operational fit for this specific stack.

---

## Decision

Apache Iceberg with PyIceberg for reads and writes.

---

## Rationale

The deciding factor is the Python client story. PyIceberg provides a
native Python catalog API that reads and writes Iceberg tables without
a JVM or a Spark session. The RCA agent needs to query the audit trail
in a lightweight Python process — spinning up a Spark context for a
50-row compliance query is not acceptable.

Delta Lake's Python library (delta-rs) has improved significantly but
at the time of this decision PyIceberg is more mature for catalog
operations, particularly namespace management and table creation via
a REST catalog.

The second factor is partition evolution. The current partitioning
scheme (by event_date) is reasonable but may need to change if
per-model partitioning becomes necessary for query performance.
Iceberg supports partition evolution without rewriting data.
Delta Lake requires rewriting the table.

The third factor is engine agnosticism. Iceberg is designed to be
read by any engine — Flink, Trino, Spark, DuckDB, PyIceberg. When
this system moves to production and an analyst needs to query the
audit trail from a SQL tool, any Iceberg-compatible engine works
without format conversion. Delta Lake has broader Spark ecosystem
support but narrower support outside it.

---

## Tradeoffs

Delta Lake has better merge (UPSERT) support via its MERGE INTO syntax.
For an append-only audit trail this doesn't matter, but if the schema
ever needs acknowledged/reviewed status updates written back to the
inference_events table, Delta Lake would be simpler for that operation.

PyIceberg's schema compatibility checking is strict. When the Arrow
schema inferred from a Python dict doesn't exactly match the Iceberg
table schema (required vs optional, type widening), writes fail with
verbose errors. This cost time during development and is a real
operational friction point.

---

## Alternatives considered

| Option | Reason not chosen |
|--------|------------------|
| Delta Lake (delta-rs) | Less mature Python catalog API at time of decision |
| Apache Hudi | Steeper learning curve, smaller Python ecosystem |
| Parquet files on S3 directly | No ACID, no schema registry, no time-travel |
