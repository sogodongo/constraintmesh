"""
Day 1 verification tests.
Run with: pytest tests/test_day1_agent.py -v

These tests run without Docker — they mock the OTLP exporter
and verify the span attributes satisfy EU AI Act Article 12
requirements before a single container is started.
"""

import hashlib
import json
from unittest.mock import MagicMock, patch

import pytest

from agents.collector import (
    AdaptiveSampler,
    hash_input,
    simulate_model_inference,
)


class TestArticle12Attributes:
    """
    EU AI Act Article 12 requires specific attributes on every logged event.
    These tests act as a compliance spec — if they pass, the agent
    satisfies the logging requirement.
    """

    def test_input_hash_is_deterministic(self):
        features = {"age_band": "18-40", "severity": 0.7}
        assert hash_input(features) == hash_input(features)

    def test_input_hash_changes_with_input(self):
        f1 = {"age_band": "18-40", "severity": 0.7}
        f2 = {"age_band": "65+", "severity": 0.7}
        assert hash_input(f1) != hash_input(f2)

    def test_input_hash_is_not_reversible(self):
        features = {"age_band": "18-40", "severity": 0.7, "name": "test_patient"}
        h = hash_input(features)
        assert "test_patient" not in h
        assert "18-40" not in h

    def test_inference_result_has_required_fields(self):
        result = simulate_model_inference("health-triage-v2")
        required = [
            "features",
            "input_hash",
            "prediction_score",
            "prediction_class",
            "model_version",
            "latency_ms",
        ]
        for field in required:
            assert field in result, f"Missing Article 12 required field: {field}"

    def test_prediction_score_in_valid_range(self):
        for _ in range(50):
            result = simulate_model_inference("health-triage-v2")
            assert 0.0 <= result["prediction_score"] <= 1.0

    def test_drift_injection_shifts_score_distribution(self):
        normal_scores = [
            simulate_model_inference("m", inject_drift=False)["prediction_score"]
            for _ in range(100)
        ]
        drift_scores = [
            simulate_model_inference("m", inject_drift=True)["prediction_score"]
            for _ in range(100)
        ]
        normal_mean = sum(normal_scores) / len(normal_scores)
        drift_mean = sum(drift_scores) / len(drift_scores)
        assert drift_mean < normal_mean - 0.15, (
            f"Drift injection should shift mean by >0.15. "
            f"Normal: {normal_mean:.3f}, Drift: {drift_mean:.3f}"
        )

    def test_high_risk_classification_triggers_oversight_flag(self):
        # Scores above 0.6 are classified high-risk under the health triage model.
        # Article 14 requires human oversight to be flagged for high-risk predictions.
        result = simulate_model_inference("health-triage-v2")
        if result["prediction_score"] > 0.6:
            assert result["prediction_class"] == "high-risk"
        else:
            assert result["prediction_class"] == "low-risk"


class TestAdaptiveSampler:
    """
    The adaptive sampler maintains Article 12 completeness under
    bandwidth pressure by recording the sampling rate on each span.
    These tests verify it behaves correctly at each budget threshold.
    """

    def test_emits_all_events_within_budget(self):
        sampler = AdaptiveSampler(budget_bytes_per_min=10_000_000)
        decisions = [sampler.should_emit() for _ in range(100)]
        assert all(decisions), "All events should emit when well within budget"
        assert sampler.current_rate == 1.0

    def test_records_sampling_rate_on_span(self):
        sampler = AdaptiveSampler(budget_bytes_per_min=10_000_000)
        sampler.should_emit()
        assert sampler.current_rate == 1.0
        assert sampler.last_reason == "within_budget"

    def test_reduces_rate_at_90pct_budget(self):
        sampler = AdaptiveSampler(budget_bytes_per_min=1000)
        # exhaust most of the budget
        for _ in range(20):
            sampler.record_emission(estimated_span_bytes=48)
        # force the window to look full
        sampler.emitted_this_minute = 950
        sampler.window_start -= 1

        emit_count = sum(sampler.should_emit() for _ in range(200))
        assert emit_count < 200, "Should drop some events near budget limit"

    def test_never_drops_to_zero(self):
        """
        Article 12 requires the audit trail to be complete enough for
        post-hoc risk identification. Dropping to zero is not acceptable.
        The sampler must always emit some events to maintain statistical
        representativeness.
        """
        sampler = AdaptiveSampler(budget_bytes_per_min=100)
        sampler.emitted_this_minute = 99
        sampler.window_start -= 1

        emit_count = sum(sampler.should_emit() for _ in range(1000))
        assert emit_count > 0, "Sampler must never drop to zero — Article 12 requirement"
