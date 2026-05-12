"""
Article 11 - Technical documentation.

When a drift alert fires, this agent queries the Iceberg audit trail,
assembles the incident context, and generates a structured RCA summary
via the Claude API. The output is written to governance.oversight_actions
and forms part of the Article 11 technical documentation package.

This is not a full LangGraph implementation - that adds complexity without
benefit at this stage. The agent pattern here is: gather context, reason,
produce structured output. LangGraph would be warranted if the agent needed
to make branching tool calls based on intermediate results.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import anthropic
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from pyiceberg.catalog.rest import RestCatalog

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "constraintmesh")
S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "constraintmesh_secret")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3://constraintmesh-iceberg")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

INPUT_TOPIC = "alerts.drift"
OUTPUT_TOPIC = "governance.oversight_actions"
CONSUMER_GROUP = "rca-agent"


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


def query_recent_inferences(catalog: RestCatalog, model_id: str, limit: int = 50) -> list[dict]:
    """
    Pulls the most recent inference records for the model from Iceberg.
    Used to give the RCA agent context beyond just the alert that fired.
    """
    try:
        table = catalog.load_table("audit.inference_events")
        df = (
            table.scan(
                row_filter=f"model_id = '{model_id}'",
                limit=limit,
            )
            .to_arrow()
            .to_pydict()
        )
        rows = []
        for i in range(len(df.get("event_id", []))):
            rows.append({
                "prediction_score": df["prediction_score"][i],
                "prediction_class": df["prediction_class"][i],
                "latency_ms": df["latency_ms"][i],
                "risk_band": df["risk_band"][i],
                "logged_at": df["logged_at"][i],
            })
        return rows
    except Exception as e:
        log.warning("failed to query inference history: %s", e)
        return []


def query_recent_alerts(catalog: RestCatalog, model_id: str, limit: int = 10) -> list[dict]:
    try:
        table = catalog.load_table("audit.drift_alerts")
        df = (
            table.scan(
                row_filter=f"model_id = '{model_id}'",
                limit=limit,
            )
            .to_arrow()
            .to_pydict()
        )
        rows = []
        for i in range(len(df.get("alert_id", []))):
            rows.append({
                "rule_fired": df["rule_fired"][i],
                "sigma_deviation": df["sigma_deviation"][i],
                "severity": df["severity"][i],
                "detected_at": df["detected_at"][i],
            })
        return rows
    except Exception as e:
        log.warning("failed to query alert history: %s", e)
        return []


def build_prompt(alert: dict, inferences: list[dict], prior_alerts: list[dict]) -> str:
    recent_scores = [r["prediction_score"] for r in inferences if r["prediction_score"] is not None]
    avg_score = round(sum(recent_scores) / len(recent_scores), 4) if recent_scores else None
    high_risk_count = sum(1 for r in inferences if r.get("prediction_class") == "high-risk")

    prior_rules = [a["rule_fired"] for a in prior_alerts]

    return f"""You are reviewing a governance alert for a high-risk AI system deployed under the EU AI Act.

System: {alert.get("model_id")}
Node: {alert.get("node_id", "unknown")}
Alert time: {alert.get("detected_at")}

Alert details:
- Rule fired: {alert.get("rule_fired")}
- Current score: {alert.get("current_score")}
- Baseline mean: {alert.get("baseline_mean")}
- Baseline stddev: {alert.get("baseline_stddev")}
- Sigma deviation: {alert.get("sigma_deviation")}
- Severity: {alert.get("severity")}

Recent inference context (last {len(inferences)} records):
- Average prediction score: {avg_score}
- High-risk classifications: {high_risk_count} of {len(inferences)}

Prior alerts on this model: {len(prior_alerts)}
Prior rules fired: {", ".join(set(prior_rules)) if prior_rules else "none"}

Write a concise incident summary for the Article 11 technical documentation record.
Include: what triggered the alert, what the data suggests about model behaviour,
whether this appears to be a transient anomaly or a sustained shift, and what
a human reviewer should check first.

Keep it under 150 words. Write plainly. No bullet points."""


def generate_rca(alert: dict, inferences: list[dict], prior_alerts: list[dict]) -> Optional[str]:
    if not ANTHROPIC_API_KEY:
        log.error("ANTHROPIC_API_KEY not set")
        return None

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt = build_prompt(alert, inferences, prior_alerts)

    try:
        message = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text.strip()
    except Exception as e:
        log.error("Claude API call failed: %s", e)
        return None


def run() -> None:
    catalog = get_catalog()

    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: v.encode("utf-8"),
    )

    log.info("rca agent started, waiting for alerts")

    for message in consumer:
        try:
            alert = json.loads(message.value)
        except json.JSONDecodeError:
            continue

        model_id = alert.get("model_id")
        if not model_id:
            continue

        log.info("processing alert model=%s rule=%s", model_id, alert.get("rule_fired"))

        inferences = query_recent_inferences(catalog, model_id)
        prior_alerts = query_recent_alerts(catalog, model_id)

        summary = generate_rca(alert, inferences, prior_alerts)
        if not summary:
            continue

        record = {
            "record_id": alert.get("alert_id"),
            "model_id": model_id,
            "alert_type": "drift",
            "rule_fired": alert.get("rule_fired"),
            "severity": alert.get("severity"),
            "rca_summary": summary,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "eu_ai_act_article": "11",
            "requires_human_review": True,
            "reviewed": False,
        }

        producer.send(OUTPUT_TOPIC, value=json.dumps(record))
        producer.flush()

        log.info("rca summary generated model=%s", model_id)
        log.info("summary: %s", summary[:120] + "..." if len(summary) > 120 else summary)


if __name__ == "__main__":
    run()
