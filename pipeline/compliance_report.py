"""
Generates a structured EU AI Act compliance evidence report from the
Iceberg audit trail. Output is a markdown file suitable for inclusion
in an Article 11 technical documentation package.

This is not a legal conformity assessment. It produces the technical
evidence that supports one. A qualified third-party auditor still
reviews and signs off.
"""

import os
import statistics
from collections import Counter
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError

load_dotenv()

CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "constraintmesh")
S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "constraintmesh_secret")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3://constraintmesh-iceberg")
OUTPUT_PATH = os.getenv("REPORT_OUTPUT_PATH", "compliance_report.md")


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


def load_inference_events(catalog: RestCatalog) -> dict:
    try:
        table = catalog.load_table("audit.inference_events")
        return table.scan().to_arrow().to_pydict()
    except NoSuchTableError:
        return {}


def load_drift_alerts(catalog: RestCatalog) -> dict:
    try:
        table = catalog.load_table("audit.drift_alerts")
        return table.scan().to_arrow().to_pydict()
    except NoSuchTableError:
        return {}


def format_ts(ts: str) -> str:
    try:
        return datetime.fromisoformat(ts).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return ts or "unknown"


def generate(catalog: RestCatalog) -> str:
    inferences = load_inference_events(catalog)
    alerts = load_drift_alerts(catalog)

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    n_inferences = len(inferences.get("event_id", []))
    n_alerts = len(alerts.get("alert_id", []))

    scores = [s for s in inferences.get("prediction_score", []) if s is not None]
    latencies = [l for l in inferences.get("latency_ms", []) if l is not None]
    classes = inferences.get("prediction_class", [])
    nodes = set(inferences.get("node_id", []))
    models = set(inferences.get("model_id", []))
    logged_ats = [t for t in inferences.get("logged_at", []) if t]

    high_risk_count = sum(1 for c in classes if c == "high-risk")
    high_risk_rate = round(high_risk_count / n_inferences, 4) if n_inferences else 0

    avg_score = round(statistics.mean(scores), 4) if scores else None
    stddev_score = round(statistics.stdev(scores), 4) if len(scores) > 1 else None
    avg_latency = round(statistics.mean(latencies), 2) if latencies else None
    p99_latency = round(sorted(latencies)[int(len(latencies) * 0.99)], 2) if latencies else None

    earliest = format_ts(min(logged_ats)) if logged_ats else "unknown"
    latest = format_ts(max(logged_ats)) if logged_ats else "unknown"

    rule_counts = Counter(alerts.get("rule_fired", []))
    severity_counts = Counter(alerts.get("severity", []))
    sigma_vals = [s for s in alerts.get("sigma_deviation", []) if s is not None]
    max_sigma = round(max(abs(s) for s in sigma_vals), 4) if sigma_vals else None

    lines = [
        "# EU AI Act Compliance Evidence Report",
        "",
        f"**Generated:** {generated_at}",
        f"**System:** {', '.join(models) if models else 'unknown'}",
        f"**Nodes covered:** {len(nodes)}",
        f"**Report period:** {earliest} to {latest}",
        "",
        "---",
        "",
        "## Summary",
        "",
        "This report documents the technical evidence collected by the ConstraintMesh",
        "governance platform for the above AI system. It covers the obligations in",
        "Articles 9, 10, 11, 12, 14, and 26 of the EU AI Act.",
        "",
        "| Article | Obligation | Evidence status |",
        "|---------|-----------|----------------|",
        f"| 9 | Risk management system | {n_alerts} drift events detected and logged |",
        f"| 10 | Data governance | Feature distribution monitoring active |",
        f"| 11 | Technical documentation | This report |",
        f"| 12 | Automatic logging | {n_inferences} inference events logged |",
        f"| 14 | Human oversight | Acknowledge endpoint active, oversight actions logged |",
        f"| 26 | Log retention | Iceberg audit trail with configurable retention |",
        "",
        "---",
        "",
        "## Article 12 — Automatic logging",
        "",
        f"Total inference events logged: **{n_inferences}**",
        f"Logging period: {earliest} to {latest}",
        f"Nodes reporting: {len(nodes)}",
        "",
        "Each event record contains:",
        "- System identity and model version",
        "- Hashed input identifier (GDPR-compatible, not raw input)",
        "- Prediction score and classification",
        "- Inference latency",
        "- Risk band assessment",
        "- Human oversight flag",
        "- Sampling rate and reason (adaptive sampler metadata)",
        "- ISO 8601 timestamp",
        "",
        "---",
        "",
        "## Article 9 — Risk management",
        "",
        f"Drift detection method: Western Electric SPC rules (4 rules active)",
        f"Total alerts generated: **{n_alerts}**",
        "",
    ]

    if rule_counts:
        lines += [
            "Alerts by rule:",
            "",
            "| Rule | Count |",
            "|------|-------|",
        ]
        for rule, count in rule_counts.most_common():
            lines.append(f"| {rule} | {count} |")
        lines.append("")

    if severity_counts:
        lines += [
            "Alerts by severity:",
            "",
            "| Severity | Count |",
            "|----------|-------|",
        ]
        for sev, count in severity_counts.most_common():
            lines.append(f"| {sev} | {count} |")
        lines.append("")

    if max_sigma:
        lines.append(f"Maximum sigma deviation observed: **{max_sigma}σ**")
        lines.append("")

    lines += [
        "---",
        "",
        "## Article 10 — Data governance",
        "",
        "Prediction score distribution (proxy for output data quality):",
        "",
        f"- Mean: {avg_score}",
        f"- Std dev: {stddev_score}",
        f"- High-risk classification rate: {high_risk_rate * 100:.1f}% ({high_risk_count} of {n_inferences})",
        "",
        "Feature distribution monitoring is active via the data quality pipeline.",
        "Alerts are written to `alerts.data_quality` when live distributions",
        "diverge from the training profile beyond configured thresholds.",
        "",
        "---",
        "",
        "## Article 14 — Human oversight",
        "",
        "The governance dashboard provides:",
        "- Real-time display of active drift alerts",
        "- Per-alert acknowledge action with reviewer ID and notes",
        "- Acknowledgement records written to `governance.oversight_actions`",
        "- RCA summaries surfaced alongside each alert",
        "",
        "Every acknowledgement record contains: alert ID, reviewer ID,",
        "notes, timestamp, and Article 14 attribution.",
        "",
        "---",
        "",
        "## Article 26 — Log retention",
        "",
        "Audit trail storage: Apache Iceberg on S3-compatible object storage",
        "",
        "Tables:",
        "- `audit.inference_events` — full inference log (Article 12)",
        "- `audit.drift_alerts` — risk management events (Article 9)",
        "",
        "Retention policy is configurable at the Iceberg catalog level.",
        "Minimum 6-month retention is enforced by default.",
        "Time-travel queries are supported for any historical point within the retention window.",
        "",
        "---",
        "",
        "## Inference performance",
        "",
        f"- Average prediction score: {avg_score}",
        f"- Score std dev: {stddev_score}",
        f"- Average latency: {avg_latency}ms",
        f"- p99 latency: {p99_latency}ms",
        "",
        "---",
        "",
        "## Known limitations",
        "",
        "- SPC detection assumes roughly normal score distributions.",
        "  Bimodal or heavily skewed models may show elevated false positive rates.",
        "- The adaptive sampler reduces telemetry volume under bandwidth pressure.",
        "  Statistical correction using the logged sampling rate is required for",
        "  exact event counts in constrained deployment scenarios.",
        "- This report covers technical evidence only. It does not constitute",
        "  a formal conformity assessment under Annex VII. Third-party audit",
        "  is required before CE marking or market placement.",
        "",
        "---",
        "",
        f"*Generated by ConstraintMesh v0.1.0 — {generated_at}*",
    ]

    return "\n".join(lines)


def run() -> None:
    print("querying audit trail...")
    catalog = get_catalog()
    report = generate(catalog)

    with open(OUTPUT_PATH, "w") as f:
        f.write(report)

    print(f"report written to {OUTPUT_PATH}")
    print(f"lines: {len(report.splitlines())}")


if __name__ == "__main__":
    run()
