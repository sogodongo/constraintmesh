import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import AsyncGenerator

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from kafka import KafkaProducer
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pydantic import BaseModel

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "constraintmesh")
S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "constraintmesh_secret")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3://constraintmesh-iceberg")

app = FastAPI(title="ConstraintMesh API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# in-memory queue for SSE - alerts get pushed here by background poller
_alert_queue: asyncio.Queue = asyncio.Queue()


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


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: v.encode("utf-8"),
    )


@app.get("/health")
def health():
    return {"status": "ok", "version": "0.2.0"}


@app.get("/metrics")
def get_metrics():
    """
    Returns aggregate inference metrics per model from the Iceberg audit trail.
    Used by the dashboard to populate SLO cards and latency charts.
    """
    try:
        catalog = get_catalog()
        table = catalog.load_table("audit.inference_events")
        df = table.scan(limit=500).to_arrow().to_pydict()

        if not df.get("model_id"):
            return {"models": []}

        from collections import defaultdict
        stats: dict = defaultdict(lambda: {
            "total": 0, "high_risk": 0, "scores": [], "latencies": []
        })

        for i in range(len(df["model_id"])):
            mid = df["model_id"][i]
            stats[mid]["total"] += 1
            if df["prediction_class"][i] == "high-risk":
                stats[mid]["high_risk"] += 1
            score = df["prediction_score"][i]
            if score is not None:
                stats[mid]["scores"].append(score)
            lat = df["latency_ms"][i]
            if lat is not None:
                stats[mid]["latencies"].append(lat)

        result = []
        for mid, s in stats.items():
            scores = s["scores"]
            latencies = s["latencies"]
            result.append({
                "model_id": mid,
                "total_inferences": s["total"],
                "high_risk_count": s["high_risk"],
                "high_risk_rate": round(s["high_risk"] / s["total"], 4) if s["total"] else 0,
                "avg_score": round(sum(scores) / len(scores), 4) if scores else None,
                "avg_latency_ms": round(sum(latencies) / len(latencies), 2) if latencies else None,
                "p99_latency_ms": round(sorted(latencies)[int(len(latencies) * 0.99)], 2) if latencies else None,
            })

        return {"models": result}

    except NoSuchTableError:
        return {"models": []}
    except Exception as e:
        log.error("metrics query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/alerts")
def get_alerts(limit: int = 20):
    """
    Returns unreviewed drift alerts from the Iceberg audit trail.
    These are the Article 9 risk events requiring human oversight (Article 14).
    """
    try:
        catalog = get_catalog()
        table = catalog.load_table("audit.drift_alerts")
        df = table.scan(limit=limit).to_arrow().to_pydict()

        alerts = []
        for i in range(len(df.get("alert_id", []))):
            alerts.append({
                "alert_id": df["alert_id"][i],
                "model_id": df["model_id"][i],
                "rule_fired": df["rule_fired"][i],
                "current_score": df["current_score"][i],
                "baseline_mean": df["baseline_mean"][i],
                "sigma_deviation": df["sigma_deviation"][i],
                "severity": df["severity"][i],
                "detected_at": df["detected_at"][i],
                "eu_ai_act_article": df["eu_ai_act_article"][i],
            })

        return {"alerts": alerts, "total": len(alerts)}

    except NoSuchTableError:
        return {"alerts": [], "total": 0}
    except Exception as e:
        log.error("alerts query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


class AcknowledgeRequest(BaseModel):
    reviewer_id: str
    notes: str = ""


@app.post("/alerts/{alert_id}/acknowledge")
def acknowledge_alert(alert_id: str, body: AcknowledgeRequest):
    """
    Logs a human oversight action for a drift alert.

    This is the Article 14 requirement: deployers must be able to
    monitor, understand, and intervene. The acknowledgement record
    proves that a human reviewed the alert and took a documented action.
    """
    record = {
        "record_id": str(uuid.uuid4()),
        "alert_id": alert_id,
        "action": "acknowledged",
        "reviewer_id": body.reviewer_id,
        "notes": body.notes,
        "acknowledged_at": datetime.now(timezone.utc).isoformat(),
        "eu_ai_act_article": "14",
    }

    try:
        producer = get_producer()
        producer.send("governance.oversight_actions", value=json.dumps(record))
        producer.flush()
        log.info("alert acknowledged alert_id=%s reviewer=%s", alert_id, body.reviewer_id)
        return {"status": "acknowledged", "record_id": record["record_id"]}
    except Exception as e:
        log.error("acknowledge failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/oversight")
def get_oversight_actions(limit: int = 20):
    """
    Returns RCA summaries from governance.oversight_actions topic.
    These are the Article 11 incident records generated by the RCA agent.
    """
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            "governance.oversight_actions",
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=f"api-oversight-reader-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
            consumer_timeout_ms=2000,
            value_deserializer=lambda v: v.decode("utf-8"),
        )
        records = []
        for msg in consumer:
            try:
                records.append(json.loads(msg.value))
            except json.JSONDecodeError:
                continue
            if len(records) >= limit:
                break
        consumer.close()
        return {"records": records, "total": len(records)}
    except Exception as e:
        log.error("oversight query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


async def _sse_event_generator() -> AsyncGenerator[str, None]:
    while True:
        try:
            alert = await asyncio.wait_for(_alert_queue.get(), timeout=15.0)
            yield f"data: {json.dumps(alert)}\n\n"
        except asyncio.TimeoutError:
            yield "data: {\"type\": \"heartbeat\"}\n\n"


@app.get("/stream")
async def stream_alerts():
    """
    SSE endpoint. Dashboard connects here to receive alerts in real time
    without polling. See ADR-004 for why SSE over WebSockets.
    """
    return StreamingResponse(
        _sse_event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
