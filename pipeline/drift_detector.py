"""
Article 9 - Risk management system.

Monitors the prediction score stream for distribution shifts using
Western Electric SPC rules. When a rule fires, writes a structured
alert to alerts.drift.

Western Electric rules detect non-random patterns in a control chart:
- Rule 1: single point beyond 3-sigma
- Rule 2: 8 consecutive points on same side of mean
- Rule 3: 6 consecutive points trending in one direction
- Rule 4: 2 out of 3 consecutive points beyond 2-sigma on same side

These are standard process control tests. They're sensitive enough to
catch gradual drift before it becomes a hard failure, without generating
noise from normal score variance.
"""

import json
import logging
import os
import statistics
import time
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
INPUT_TOPIC = "otel.spans"
OUTPUT_TOPIC = "alerts.drift"
CONSUMER_GROUP = "drift-detector"

# Minimum observations before SPC rules apply. Below this the mean and
# stddev estimates are too noisy to be useful.
MIN_OBSERVATIONS = 30

# Baseline window. The detector uses the first N observations to establish
# the control limits, then applies rules on subsequent observations.
BASELINE_WINDOW = 100


@dataclass
class DriftAlert:
    alert_id: str
    model_id: str
    node_id: str
    rule_fired: str
    current_score: float
    baseline_mean: float
    baseline_stddev: float
    sigma_deviation: float
    consecutive_count: int
    detected_at: str
    eu_ai_act_article: str = "9"
    severity: str = "warning"

    def to_json(self) -> str:
        return json.dumps(asdict(self))


class SPCMonitor:
    """
    Maintains a rolling window of prediction scores and applies
    Western Electric detection rules.

    The baseline mean and stddev are computed from the first
    BASELINE_WINDOW observations. Control limits are fixed after
    that point so that drift is measured against the model's
    established behaviour, not a continuously shifting window.
    """

    def __init__(self, model_id: str):
        self.model_id = model_id
        self.baseline_scores: list[float] = []
        self.baseline_locked = False
        self.mean: float = 0.0
        self.stddev: float = 1.0

        # Rolling windows for rule evaluation
        self.recent: deque[float] = deque(maxlen=8)
        self.recent_3: deque[float] = deque(maxlen=3)

        self.observations = 0

    def _lock_baseline(self) -> None:
        self.mean = statistics.mean(self.baseline_scores)
        self.stddev = statistics.stdev(self.baseline_scores)
        self.baseline_locked = True
        log.info(
            "baseline locked model=%s mean=%.4f stddev=%.4f n=%d",
            self.model_id, self.mean, self.stddev, len(self.baseline_scores)
        )

    def add(self, score: float) -> Optional[DriftAlert]:
        self.observations += 1

        if not self.baseline_locked:
            self.baseline_scores.append(score)
            if len(self.baseline_scores) >= BASELINE_WINDOW:
                self._lock_baseline()
            return None

        self.recent.append(score)
        self.recent_3.append(score)

        return self._evaluate(score)

    def _sigma(self, score: float) -> float:
        if self.stddev == 0:
            return 0.0
        return (score - self.mean) / self.stddev

    def _evaluate(self, score: float) -> Optional[DriftAlert]:
        sigma = self._sigma(score)

        # Rule 1: beyond 3 sigma
        if abs(sigma) > 3.0:
            return self._alert("rule_1_beyond_3sigma", score, sigma, 1)

        # Rule 2: 8 consecutive on same side of mean
        if len(self.recent) == 8:
            above = sum(1 for s in self.recent if s > self.mean)
            below = sum(1 for s in self.recent if s < self.mean)
            if above == 8 or below == 8:
                return self._alert("rule_2_eight_consecutive_same_side", score, sigma, 8)

        # Rule 3: 6 consecutive trending
        if len(self.recent) >= 6:
            window = list(self.recent)[-6:]
            if all(window[i] < window[i+1] for i in range(5)):
                return self._alert("rule_3_six_trending_up", score, sigma, 6)
            if all(window[i] > window[i+1] for i in range(5)):
                return self._alert("rule_3_six_trending_down", score, sigma, 6)

        # Rule 4: 2 of 3 beyond 2 sigma on same side
        if len(self.recent_3) == 3:
            beyond_2sigma_above = sum(1 for s in self.recent_3 if self._sigma(s) > 2.0)
            beyond_2sigma_below = sum(1 for s in self.recent_3 if self._sigma(s) < -2.0)
            if beyond_2sigma_above >= 2 or beyond_2sigma_below >= 2:
                return self._alert("rule_4_two_of_three_beyond_2sigma", score, sigma, 3)

        return None

    def _alert(
        self, rule: str, score: float, sigma: float, consecutive: int
    ) -> DriftAlert:
        import uuid
        severity = "critical" if abs(sigma) > 4.0 else "warning"
        return DriftAlert(
            alert_id=str(uuid.uuid4()),
            model_id=self.model_id,
            node_id="",
            rule_fired=rule,
            current_score=round(score, 4),
            baseline_mean=round(self.mean, 4),
            baseline_stddev=round(self.stddev, 4),
            sigma_deviation=round(sigma, 4),
            consecutive_count=consecutive,
            detected_at=datetime.now(timezone.utc).isoformat(),
            severity=severity,
        )


def extract_span_attributes(raw_value: str) -> Optional[dict]:
    """
    Pulls the relevant attributes out of an OTLP JSON span batch.
    Returns None if the message doesn't contain inference spans.
    """
    try:
        payload = json.loads(raw_value)
        resource_spans = payload.get("resourceSpans", [])

        results = []
        for rs in resource_spans:
            resource_attrs = {
                a["key"]: list(a["value"].values())[0]
                for a in rs.get("resource", {}).get("attributes", [])
            }
            for scope_span in rs.get("scopeSpans", []):
                for span in scope_span.get("spans", []):
                    span_attrs = {
                        a["key"]: list(a["value"].values())[0]
                        for a in span.get("attributes", [])
                    }
                    results.append({**resource_attrs, **span_attrs})

        return results if results else None

    except (json.JSONDecodeError, KeyError, StopIteration):
        return None


def run() -> None:
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

    monitors: dict[str, SPCMonitor] = {}

    log.info("drift detector started bootstrap=%s topic=%s", KAFKA_BOOTSTRAP, INPUT_TOPIC)

    for message in consumer:
        spans = extract_span_attributes(message.value)
        if not spans:
            continue

        for attrs in spans:
            model_id = attrs.get("ai.model.system_id")
            score = attrs.get("ai.inference.prediction_score")
            node_id = attrs.get("ai.model.node_id", "")

            if not model_id or score is None:
                continue

            if model_id not in monitors:
                monitors[model_id] = SPCMonitor(model_id)
                log.info("new monitor registered model=%s", model_id)

            alert = monitors[model_id].add(float(score))

            if alert:
                alert.node_id = node_id
                producer.send(OUTPUT_TOPIC, value=alert.to_json())
                producer.flush()
                log.warning(
                    "drift alert fired model=%s rule=%s score=%.4f sigma=%.4f severity=%s",
                    alert.model_id,
                    alert.rule_fired,
                    alert.current_score,
                    alert.sigma_deviation,
                    alert.severity,
                )


if __name__ == "__main__":
    run()
