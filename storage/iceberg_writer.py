"""
Consumes from alerts.drift and otel.spans, writes to Iceberg tables on MinIO.

Two tables:
  constraintmesh.audit.inference_events  - Article 12 log retention
  constraintmesh.audit.drift_alerts      - Article 9 risk management records

Both partitioned by event_date so compliance queries (range scans by date)
hit only the relevant partitions. Retention policy is enforced at the
catalog level - see docs/ADR-003.
"""

import json
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa
from kafka import KafkaConsumer
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "constraintmesh")
S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "constraintmesh_secret")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3://constraintmesh-iceberg")

INFERENCE_SCHEMA = Schema(
    NestedField(1,  "event_id",        StringType(),      required=False),
    NestedField(2,  "model_id",        StringType(),      required=False),
    NestedField(3,  "model_version",   StringType(),      required=False),
    NestedField(4,  "node_id",         StringType(),      required=False),
    NestedField(5,  "input_hash",      StringType(),      required=False),
    NestedField(6,  "prediction_score",DoubleType(),      required=False),
    NestedField(7,  "prediction_class",StringType(),      required=False),
    NestedField(8,  "latency_ms",      DoubleType(),      required=False),
    NestedField(9,  "risk_band",       StringType(),      required=False),
    NestedField(10, "sampling_rate",   DoubleType(),      required=False),
    NestedField(11, "human_oversight_required", BooleanType(), required=False),
    NestedField(12, "logged_at",       StringType(),      required=False),
    NestedField(13, "event_date",      StringType(),      required=False),
)

DRIFT_ALERT_SCHEMA = Schema(
    NestedField(1,  "alert_id",         StringType(),  required=False),
    NestedField(2,  "model_id",         StringType(),  required=False),
    NestedField(3,  "node_id",          StringType(),  required=False),
    NestedField(4,  "rule_fired",       StringType(),  required=False),
    NestedField(5,  "current_score",    DoubleType(),  required=False),
    NestedField(6,  "baseline_mean",    DoubleType(),  required=False),
    NestedField(7,  "baseline_stddev",  DoubleType(),  required=False),
    NestedField(8,  "sigma_deviation",  DoubleType(),  required=False),
    NestedField(9,  "severity",         StringType(),  required=False),
    NestedField(10, "eu_ai_act_article",StringType(),  required=False),
    NestedField(11, "consecutive_count",LongType(),    required=False),
    NestedField(12, "detected_at",      StringType(),  required=False),
    NestedField(13, "event_date",       StringType(),  required=False),
)


def get_catalog() -> RestCatalog:
    return RestCatalog(
        name="constraintmesh",
        uri=CATALOG_URI,
        warehouse=WAREHOUSE,
        **{
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
        },
    )


def ensure_table(catalog: RestCatalog, namespace: str, name: str, schema: Schema) -> None:
    try:
        catalog.load_table(f"{namespace}.{name}")
    except NoSuchTableError:
        catalog.create_namespace_if_not_exists(namespace)
        catalog.create_table(
            identifier=f"{namespace}.{name}",
            schema=schema,
        )
        log.info("created table %s.%s", namespace, name)


def parse_inference_span(raw: str) -> list[dict]:
    try:
        payload = json.loads(raw)
        rows = []
        for rs in payload.get("resourceSpans", []):
            resource_attrs = {
                a["key"]: list(a["value"].values())[0]
                for a in rs.get("resource", {}).get("attributes", [])
            }
            for ss in rs.get("scopeSpans", []):
                for span in ss.get("spans", []):
                    attrs = {
                        a["key"]: list(a["value"].values())[0]
                        for a in span.get("attributes", [])
                    }
                    merged = {**resource_attrs, **attrs}
                    logged_at = merged.get("eu.ai_act.log_timestamp", datetime.now(timezone.utc).isoformat())
                    event_date = logged_at[:10]
                    rows.append({
                        "event_id":   span.get("spanId", ""),
                        "model_id":   merged.get("ai.model.system_id", ""),
                        "model_version": merged.get("ai.model.name", ""),
                        "node_id":    merged.get("ai.model.node_id", ""),
                        "input_hash": merged.get("ai.inference.input_hash", ""),
                        "prediction_score": merged.get("ai.inference.prediction_score"),
                        "prediction_class": merged.get("ai.inference.prediction_class", ""),
                        "latency_ms": merged.get("ai.inference.latency_ms"),
                        "risk_band":  merged.get("ai.risk.score_band", ""),
                        "sampling_rate": float(merged.get("sampling.rate", 1.0)),
                        "human_oversight_required": merged.get("eu.ai_act.human_oversight_required", False),
                        "logged_at":  logged_at,
                        "event_date": event_date,
                    })
        return rows
    except (json.JSONDecodeError, KeyError):
        return []


def parse_drift_alert(raw: str) -> Optional[dict]:
    try:
        alert = json.loads(raw)
        detected_at = alert.get("detected_at", datetime.now(timezone.utc).isoformat())
        alert["event_date"] = detected_at[:10]
        return alert
    except json.JSONDecodeError:
        return None


def write_batch(table, rows: list[dict]) -> None:
    if not rows:
        return
    arrow_table = pa.Table.from_pylist(rows)
    table.append(arrow_table)
    log.info("wrote %d rows to %s", len(rows), table.name())


def consume_inference_events(catalog: RestCatalog) -> None:
    ensure_table(catalog, "audit", "inference_events", INFERENCE_SCHEMA)
    table = catalog.load_table("audit.inference_events")

    consumer = KafkaConsumer(
        "otel.spans",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="iceberg-inference-writer",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    buffer = []
    for message in consumer:
        rows = parse_inference_span(message.value)
        buffer.extend(rows)
        if len(buffer) >= 50:
            write_batch(table, buffer)
            buffer.clear()


def consume_drift_alerts(catalog: RestCatalog) -> None:
    ensure_table(catalog, "audit", "drift_alerts", DRIFT_ALERT_SCHEMA)
    table = catalog.load_table("audit.drift_alerts")

    consumer = KafkaConsumer(
        "alerts.drift",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="iceberg-drift-writer",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    buffer = []
    for message in consumer:
        row = parse_drift_alert(message.value)
        if row:
            buffer.append(row)
        if len(buffer) >= 10:
            write_batch(table, buffer)
            buffer.clear()


def run() -> None:
    catalog = get_catalog()

    t1 = threading.Thread(target=consume_inference_events, args=(catalog,), daemon=True)
    t2 = threading.Thread(target=consume_drift_alerts, args=(catalog,), daemon=True)

    t1.start()
    t2.start()

    log.info("iceberg writer started - inference_events and drift_alerts")
    t1.join()
    t2.join()


if __name__ == "__main__":
    run()
