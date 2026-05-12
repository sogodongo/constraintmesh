import json
import uuid
from unittest.mock import MagicMock, patch

import pytest

from pipeline.drift_detector import DriftAlert, SPCMonitor, extract_span_attributes
from pipeline.data_quality import DataQualityAlert, FeatureQualityMonitor


class TestSPCMonitor:

    def _build_monitor(self, scores: list[float]) -> SPCMonitor:
        m = SPCMonitor("test-model")
        for s in scores:
            m.add(s)
        return m

    def _baseline_scores(self, n=100, mean=0.78, stddev=0.08) -> list[float]:
        import random
        random.seed(42)
        return [max(0.0, min(1.0, random.gauss(mean, stddev))) for _ in range(n)]

    def test_no_alert_before_baseline_locked(self):
        m = SPCMonitor("test-model")
        for i in range(99):
            result = m.add(0.5)
            assert result is None

    def test_baseline_locks_after_100_observations(self):
        scores = self._baseline_scores(100)
        m = self._build_monitor(scores)
        assert m.baseline_locked
        assert 0.7 < m.mean < 0.86

    def test_rule1_fires_on_extreme_outlier(self):
        scores = self._baseline_scores(100)
        m = self._build_monitor(scores)
        alert = m.add(0.01)
        assert alert is not None
        assert alert.rule_fired == "rule_1_beyond_3sigma"
        assert alert.sigma_deviation < -3.0

    def test_rule1_does_not_fire_on_normal_score(self):
        scores = self._baseline_scores(100)
        m = self._build_monitor(scores)
        alert = m.add(m.mean)
        assert alert is None

    def test_rule2_fires_on_eight_consecutive_below_mean(self):
        scores = self._baseline_scores(100)
        m = self._build_monitor(scores)
        low_score = m.mean - 0.05
        alert = None
        for _ in range(8):
            alert = m.add(low_score)
        assert alert is not None
        assert "rule_2" in alert.rule_fired

    def test_rule3_fires_on_six_consecutive_increasing(self):
        scores = self._baseline_scores(100)
        m = self._build_monitor(scores)
        base = m.mean - 0.1
        alert = None
        for i in range(6):
            alert = m.add(base + i * 0.04)
        assert alert is not None
        assert "rule_3" in alert.rule_fired

    def test_alert_contains_required_article9_fields(self):
        scores = self._baseline_scores(100)
        m = self._build_monitor(scores)
        alert = m.add(0.01)
        assert alert is not None
        assert alert.eu_ai_act_article == "9"
        assert alert.alert_id
        assert alert.detected_at
        assert alert.baseline_mean > 0
        assert alert.baseline_stddev > 0

    def test_critical_severity_on_extreme_deviation(self):
        scores = self._baseline_scores(100)
        m = self._build_monitor(scores)
        alert = m.add(0.001)
        assert alert is not None
        assert alert.severity == "critical"




class TestExtractSpanAttributes:

    def _make_span_payload(self, score: float, model_id: str = "health-triage-v2") -> str:
        return json.dumps({
            "resourceSpans": [{
                "resource": {
                    "attributes": [
                        {"key": "eu.ai_act.system_id", "value": {"stringValue": model_id}},
                        {"key": "deployment.node_id", "value": {"stringValue": "node-001"}},
                    ]
                },
                "scopeSpans": [{
                    "scope": {"name": "constraintmesh.agent"},
                    "spans": [{
                        "name": "model.inference",
                        "attributes": [
                            {"key": "ai.model.system_id", "value": {"stringValue": model_id}},
                            {"key": "ai.model.node_id", "value": {"stringValue": "node-001"}},
                            {"key": "ai.inference.prediction_score", "value": {"doubleValue": score}},
                        ]
                    }]
                }]
            }]
        })

    def test_extracts_model_id_and_score(self):
        payload = self._make_span_payload(0.75)
        result = extract_span_attributes(payload)
        assert result is not None
        assert len(result) == 1
        assert result[0]["ai.model.system_id"] == "health-triage-v2"
        assert result[0]["ai.inference.prediction_score"] == 0.75

    def test_returns_none_on_invalid_json(self):
        result = extract_span_attributes("not json")
        assert result is None

    def test_returns_none_on_empty_spans(self):
        result = extract_span_attributes(json.dumps({"resourceSpans": []}))
        assert result is None


class TestFeatureQualityMonitor:

    def _default_profile(self) -> dict:
        return {
            "features": {
                "symptom_severity": {
                    "type": "continuous",
                    "min": 0.0,
                    "max": 1.0,
                    "max_out_of_range_rate": 0.01,
                }
            }
        }

    def test_no_alert_before_check_interval(self):
        m = FeatureQualityMonitor("test-model", self._default_profile())
        for i in range(49):
            alerts = m.record({"symptom_severity": 0.5})
            assert alerts == []

    def test_no_alert_on_clean_data(self):
        m = FeatureQualityMonitor("test-model", self._default_profile())
        for i in range(50):
            alerts = m.record({"symptom_severity": 0.5})
        assert all(len(a) == 0 for a in [alerts])

    def test_alert_fires_on_high_out_of_range_rate(self):
        m = FeatureQualityMonitor("test-model", self._default_profile())
        for i in range(49):
            m.record({"symptom_severity": 1.5})
        alerts = m.record({"symptom_severity": 1.5})
        assert len(alerts) > 0
        assert alerts[0].check_type == "continuous_out_of_range"
        assert alerts[0].eu_ai_act_article == "10"

    def test_alert_contains_required_fields(self):
        m = FeatureQualityMonitor("test-model", self._default_profile())
        for i in range(50):
            m.record({"symptom_severity": 1.5})
        alerts = m.record({"symptom_severity": 1.5})
        if alerts:
            a = alerts[0]
            assert a.alert_id
            assert a.detected_at
            assert a.feature_name == "symptom_severity"
            assert a.observation_count > 0



class TestSPCMonitorDriftSensitivity:

    def _build_baseline(self, mean=0.78, std=0.05, n=100, seed=42) -> SPCMonitor:
        import random
        random.seed(seed)
        m = SPCMonitor("test-model")
        for _ in range(n):
            m.add(max(0.1, min(0.9, random.gauss(mean, std))))
        return m

    def test_detects_mean_shift_within_50_observations(self):
        import random
        m = self._build_baseline()
        random.seed(7)
        # shift mean down by 3 stddevs - should fire quickly
        alerts = []
        for _ in range(50):
            a = m.add(max(0.1, min(0.9, random.gauss(0.63, 0.05))))
            if a:
                alerts.append(a)
        assert len(alerts) > 0, "Detector missed a 3-stddev mean shift over 50 observations"

    def test_drift_alert_references_baseline(self):
        import random
        m = self._build_baseline()
        random.seed(7)
        for _ in range(50):
            a = m.add(0.4)
            if a:
                assert a.baseline_mean > 0.5
                assert a.baseline_stddev > 0
                break
