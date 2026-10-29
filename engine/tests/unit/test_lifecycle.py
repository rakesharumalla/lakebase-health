from datetime import datetime, timedelta, timezone

from lakebase_health.checkspec import parse_check
from lakebase_health.fingerprint import fingerprint
from lakebase_health.lifecycle import apply_run
from lakebase_health.models import FLEET, CheckOutcome, CheckResult

NOW = datetime(2026, 10, 27, 12, tzinfo=timezone.utc)
SPEC = parse_check({"id": "c1", "title": "T", "category": "cat", "source": "postgres", "explain": "E",
                    "sql": "SELECT 1", "thresholds": {"warning": 0}}, stem="c1")
SPECS = {"c1": SPEC}


def res(obj="a", sev="warning", ep="ep1"):
    fp = fingerprint("c1", "p1", ep, obj)
    return CheckResult("c1", fp, "p1", "b1", ep, obj, 1.0, sev, "high", {"k": 1})


def run(existing, outcomes, now=NOW):
    changed = apply_run(existing, outcomes, SPECS, now)
    existing.update({f.fingerprint: f for f in changed})
    return changed


def test_fingerprint_is_stable_and_endpoint_specific():
    assert fingerprint("c", "p", "e1", "o") == fingerprint("c", "p", "e1", "o")
    assert fingerprint("c", "p", "e1", "o") != fingerprint("c", "p", "e2", "o")
    assert len(fingerprint("c", None, None, "o")) == 32


def test_new_then_open_then_resolved_after_two_clean_runs():
    findings = {}
    [f] = run(findings, [CheckOutcome("c1", ("p1", "ep1"), "ok", results=[res()])])
    assert f.status == "new" and f.occurrences == 1
    [f] = run(findings, [CheckOutcome("c1", ("p1", "ep1"), "ok", results=[res()])], NOW + timedelta(hours=1))
    assert f.status == "open" and f.occurrences == 2
    [f] = run(findings, [CheckOutcome("c1", ("p1", "ep1"), "ok")], NOW + timedelta(hours=2))
    assert f.status == "open" and f.clean_runs == 1
    [f] = run(findings, [CheckOutcome("c1", ("p1", "ep1"), "ok")], NOW + timedelta(hours=3))
    assert f.status == "resolved" and f.resolved_at == NOW + timedelta(hours=3)


def test_skipped_error_timeout_never_resolve():
    findings = {}
    run(findings, [CheckOutcome("c1", ("p1", "ep1"), "ok", results=[res()])])
    for status in ("skipped:compute_inactive", "error", "timeout", "skipped:not_enrolled"):
        assert run(findings, [CheckOutcome("c1", ("p1", "ep1"), status)]) == []
    assert next(iter(findings.values())).status == "new"


def test_other_scope_does_not_resolve():
    findings = {}
    run(findings, [CheckOutcome("c1", ("p1", "ep1"), "ok", results=[res()])])
    for _ in range(3):
        run(findings, [CheckOutcome("c1", ("p1", "ep2"), "ok")])
    assert next(iter(findings.values())).status == "new"


def test_fleet_scope_resolves_any_endpoint():
    findings = {}
    run(findings, [CheckOutcome("c1", FLEET, "ok", results=[res()])])
    run(findings, [CheckOutcome("c1", FLEET, "ok")])
    run(findings, [CheckOutcome("c1", FLEET, "ok")])
    assert next(iter(findings.values())).status == "resolved"


def test_resolved_finding_reopens_as_new():
    findings = {}
    run(findings, [CheckOutcome("c1", FLEET, "ok", results=[res()])])
    run(findings, [CheckOutcome("c1", FLEET, "ok")])
    run(findings, [CheckOutcome("c1", FLEET, "ok")])
    [f] = run(findings, [CheckOutcome("c1", FLEET, "ok", results=[res()])])
    assert f.status == "new" and f.resolved_at is None and f.occurrences == 1


def test_suppression_until_expiry():
    findings = {}
    [f] = run(findings, [CheckOutcome("c1", FLEET, "ok", results=[res()])])
    f.status, f.suppressed_until = "suppressed", NOW + timedelta(days=1)
    [f] = run(findings, [CheckOutcome("c1", FLEET, "ok", results=[res()])], NOW + timedelta(hours=1))
    assert f.status == "suppressed"
    [f] = run(findings, [CheckOutcome("c1", FLEET, "ok", results=[res()])], NOW + timedelta(days=2))
    assert f.status == "open"


def test_duplicate_rows_keep_highest_severity():
    [f] = apply_run({}, [CheckOutcome("c1", FLEET, "ok", results=[res(sev="warning"), res(sev="critical")])], SPECS, NOW)
    assert f.severity == "critical"


def test_naive_timestamps_from_spark_are_treated_as_utc():
    findings = {}
    [f] = run(findings, [CheckOutcome("c1", FLEET, "ok", results=[res()])])
    f.status, f.suppressed_until = "suppressed", (NOW + timedelta(days=1)).replace(tzinfo=None)
    f.first_seen = f.first_seen.replace(tzinfo=None)
    [f] = run(findings, [CheckOutcome("c1", FLEET, "ok", results=[res()])], NOW + timedelta(hours=1))
    assert f.status == "suppressed"
