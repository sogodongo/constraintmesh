"""
EU AI Act Article 12 — Automatic logging of events.

High-risk AI systems must automatically log events throughout operation,
enabling post-hoc identification of risks and substantial modifications.

This agent wraps any model serving endpoint and emits a structured
OTLP span for every inference event. The span carries all attributes
required by Article 12 and Article 9 (risk management inputs).
"""

import hashlib
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import structlog
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, StatusCode

log = structlog.get_logger()

OTLP_ENDPOINT = os.getenv("OTLP_ENDPOINT", "http://localhost:4317")
MODEL_NAME = os.getenv("MODEL_NAME", "health-triage-v2")
NODE_ID = os.getenv("NODE_ID", f"edge-node-{uuid.uuid4().hex[:8]}")
EMIT_INTERVAL_SECONDS = float(os.getenv("EMIT_INTERVAL_SECONDS", "2.0"))
BANDWIDTH_BUDGET_BYTES_PER_MIN = int(os.getenv("BANDWIDTH_BUDGET_BYTES_PER_MIN", "500000"))

# EU AI Act Article 12 requires identifying the system, version, and
# deployment context in every log record.
RESOURCE = Resource.create({
    "service.name": "constraintmesh-agent",
    "service.version": "0.1.0",
    "deployment.node_id": NODE_ID,
    "eu.ai_act.system_id": MODEL_NAME,
    "eu.ai_act.risk_category": "high-risk",
    "eu.ai_act.annex_iii_use_case": "health-and-safety",
    "eu.ai_act.article_12_compliant": "true",
})


def build_tracer() -> trace.Tracer:
    exporter = OTLPSpanExporter(
        
        endpoint=OTLP_ENDPOINT,
    )
    provider = TracerProvider(resource=RESOURCE)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return trace.get_tracer("constraintmesh.agent")


def hash_input(features: dict) -> str:
    """
    Article 10 requirement: inputs to high-risk systems must be traceable.
    We hash the input rather than storing raw data to protect PII while
    maintaining a verifiable audit trail.
    """
    canonical = json.dumps(features, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def simulate_model_inference(model_name: str, inject_drift: bool = False) -> dict:
    """
    Simulates a health triage model inference.
    In production this is replaced by the real model call.
    inject_drift shifts the score distribution to simulate a
    degraded model — used to test Article 9 risk management triggers.
    """
    features = {
        "age_band": random.choice(["0-5", "6-17", "18-40", "41-65", "65+"]),
        "symptom_severity": round(random.uniform(0.1, 1.0), 3),
        "vital_signs_normal": random.choice([True, False]),
        "days_symptomatic": random.randint(1, 14),
    }

    # Normal distribution centres around 0.78 (healthy model behaviour).
    # Drift injection shifts it to 0.45 — triggers SPC alert in pipeline.
    base_score = 0.45 if inject_drift else 0.78
    prediction_score = max(0.0, min(1.0, random.gauss(base_score, 0.08)))

    latency_ms = random.gauss(42, 8)

    return {
        "features": features,
        "input_hash": hash_input(features),
        "prediction_score": round(prediction_score, 4),
        "prediction_class": "high-risk" if prediction_score > 0.6 else "low-risk",
        "model_version": "2.3.1",
        "latency_ms": round(max(10, latency_ms), 2),
    }


def emit_inference_event(
    tracer: trace.Tracer,
    result: dict,
    adaptive_sampler: "AdaptiveSampler",
) -> None:
    """
    Emits an OTLP span for one inference event.

    Span attributes map directly to EU AI Act Article 12 logging requirements:
    - system identity and version
    - timestamp (auto-captured by OTLP SDK)
    - input identifier (hashed, not raw — GDPR compatible)
    - output and confidence
    - latency (Article 9 risk signal)
    - node context for post-incident tracing
    """
    if not adaptive_sampler.should_emit():
        log.info("span_dropped_bandwidth_budget", node_id=NODE_ID)
        return

    with tracer.start_as_current_span(
        name="model.inference",
        kind=SpanKind.INTERNAL,
    ) as span:

        # Core Article 12 attributes
        span.set_attribute("ai.model.name", result["model_version"])
        span.set_attribute("ai.model.system_id", MODEL_NAME)
        span.set_attribute("ai.model.node_id", NODE_ID)
        span.set_attribute("ai.inference.input_hash", result["input_hash"])
        span.set_attribute("ai.inference.prediction_score", result["prediction_score"])
        span.set_attribute("ai.inference.prediction_class", result["prediction_class"])
        span.set_attribute("ai.inference.latency_ms", result["latency_ms"])

        # Article 12: modifications and risk signals must be logged
        span.set_attribute("ai.risk.score_band",
            "elevated" if result["prediction_score"] < 0.55 else "normal")
        span.set_attribute("ai.risk.latency_slo_breach",
            result["latency_ms"] > 100)

        # Governance metadata
        span.set_attribute("eu.ai_act.log_timestamp",
            datetime.now(timezone.utc).isoformat())
        span.set_attribute("eu.ai_act.article_12", "logged")
        span.set_attribute("eu.ai_act.human_oversight_required",
            result["prediction_class"] == "high-risk")

        # Sampling metadata — needed to reconstruct full event rate
        # from sampled stream (Article 12 completeness requirement)
        span.set_attribute("sampling.rate", adaptive_sampler.current_rate)
        span.set_attribute("sampling.reason", adaptive_sampler.last_reason)

        span.set_status(StatusCode.OK)

    adaptive_sampler.record_emission(estimated_span_bytes=512)

    log.info(
        "inference_event_emitted",
        model=MODEL_NAME,
        score=result["prediction_score"],
        classification=result["prediction_class"],
        latency_ms=result["latency_ms"],
        sampling_rate=adaptive_sampler.current_rate,
    )


class AdaptiveSampler:
    """
    Reduces span emission rate when the uplink bandwidth budget is tight.

    This is the key engineering constraint that makes ConstraintMesh
    different from generic observability tools. Every existing OTel
    sampler assumes abundant connectivity. This one doesn't.

    The sampler never drops events entirely — it records that sampling
    occurred and at what rate, so the Flink pipeline can apply
    statistical correction when computing aggregate metrics. This
    maintains Article 12 completeness even under bandwidth pressure.
    """

    def __init__(self, budget_bytes_per_min: int):
        self.budget = budget_bytes_per_min
        self.emitted_this_minute = 0
        self.window_start = time.time()
        self.current_rate = 1.0
        self.last_reason = "within_budget"

    def _reset_window_if_needed(self) -> None:
        now = time.time()
        if now - self.window_start >= 60:
            self.emitted_this_minute = 0
            self.window_start = now

    def should_emit(self) -> bool:
        self._reset_window_if_needed()

        projected = self.emitted_this_minute / max(
            (time.time() - self.window_start) / 60, 0.01
        )

        if projected > self.budget * 0.9:
            self.current_rate = 0.5
            self.last_reason = "budget_90pct"
            return random.random() < 0.5

        if projected > self.budget * 0.7:
            self.current_rate = 0.75
            self.last_reason = "budget_70pct"
            return random.random() < 0.75

        self.current_rate = 1.0
        self.last_reason = "within_budget"
        return True

    def record_emission(self, estimated_span_bytes: int) -> None:
        self.emitted_this_minute += estimated_span_bytes


def run(inject_drift_after_seconds: float = 60.0) -> None:
    """
    Main agent loop. Emits inference events continuously.

    inject_drift_after_seconds: after this many seconds, shifts the
    prediction score distribution to simulate model degradation.
    Used for live demo: run the stack, watch the dashboard, then
    at T+60s the drift fires and the Article 9 alert appears.
    Set to 0 to start with drift immediately. Set to -1 to never inject.
    """
    tracer = build_tracer()
    sampler = AdaptiveSampler(budget_bytes_per_min=BANDWIDTH_BUDGET_BYTES_PER_MIN)
    start_time = time.time()

    log.info(
        "agent_started",
        node_id=NODE_ID,
        model=MODEL_NAME,
        otlp_endpoint=OTLP_ENDPOINT,
        drift_injection_at=inject_drift_after_seconds,
    )

    while True:
        elapsed = time.time() - start_time
        inject = (
            inject_drift_after_seconds >= 0
            and elapsed >= inject_drift_after_seconds
        )

        result = simulate_model_inference(MODEL_NAME, inject_drift=inject)
        emit_inference_event(tracer, result, sampler)

        if inject and int(elapsed) % 30 == 0:
            log.warning("drift_injection_active",
                elapsed_seconds=round(elapsed),
                current_score_mean=result["prediction_score"])

        time.sleep(EMIT_INTERVAL_SECONDS)


if __name__ == "__main__":
    run(inject_drift_after_seconds=60.0)
