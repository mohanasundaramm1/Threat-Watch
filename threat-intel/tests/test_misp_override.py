"""
Critical Test 3: MISP Override Consistency
Verifies that domains known to be in MISP are always exported as CRITICAL
(score 1.0, riskLevel CRITICAL, source misp_osint) — the zero-false-negative guarantee.
"""
import json
import os
import pytest

PUBLIC_DATA = os.path.join(os.path.dirname(__file__), "..", "..", "dashboard", "public", "data")


def _load(filename):
    path = os.path.join(PUBLIC_DATA, filename)
    if not os.path.exists(path):
        pytest.skip(f"{filename} not found — run predict_and_export.py first")
    with open(path) as f:
        return json.load(f)


@pytest.fixture(scope="module")
def threats():
    return _load("threats.json")


def test_misp_source_is_always_critical(threats):
    """Any threat with source=misp_osint must be riskLevel CRITICAL."""
    misp_threats = [t for t in threats if t.get("source") == "misp_osint"]
    if not misp_threats:
        pytest.skip("No misp_osint threats in threats.json for this date")
    failures = [t for t in misp_threats if t["riskLevel"] != "CRITICAL"]
    assert not failures, \
        f"{len(failures)} misp_osint threats are NOT riskLevel CRITICAL: " \
        f"{[t['domain'] for t in failures[:5]]}"


def test_misp_score_is_always_1(threats):
    """Any threat with source=misp_osint must have riskScore == 1.0."""
    misp_threats = [t for t in threats if t.get("source") == "misp_osint"]
    if not misp_threats:
        pytest.skip("No misp_osint threats in threats.json for this date")
    failures = [t for t in misp_threats if t["riskScore"] != 1.0]
    assert not failures, \
        f"{len(failures)} misp_osint threats have riskScore != 1.0: " \
        f"{[(t['domain'], t['riskScore']) for t in failures[:5]]}"


def test_no_misp_in_low_risk(threats):
    """MISP threats must NEVER appear as LOW risk."""
    low_misp = [t for t in threats
                if t.get("source") == "misp_osint" and t["riskLevel"] == "LOW"]
    assert not low_misp, \
        f"Found {len(low_misp)} MISP threats classified as LOW: " \
        f"{[t['domain'] for t in low_misp[:5]]}"


def test_all_critical_have_score_above_threshold(threats):
    """All CRITICAL-labeled threats should have score >= 0.9 or be MISP overrides."""
    failures = []
    for t in threats:
        if t["riskLevel"] == "CRITICAL":
            if t["riskScore"] < 0.9 and t.get("source") != "misp_osint":
                failures.append((t["domain"], t["riskScore"]))
    assert not failures, \
        f"CRITICAL threats with score < 0.9 and not MISP override: {failures[:5]}"
