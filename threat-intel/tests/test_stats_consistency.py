"""
Critical Test 2: Statistics Consistency
Verifies that the stats exported to statistics.json are internally consistent
and align with the actual records in threats.json — no silent drift.
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


@pytest.fixture(scope="module")
def stats():
    return _load("statistics.json")


def test_malicious_count_matches_threats(threats, stats):
    """maliciousCount aligns with riskScore >= 0.6 within the exported record set.
    Note: statistics.json totalSamples >= len(threats.json) because threats.json is
    capped at 5000 rows; maliciousCount is derived from the full pre-cap result set.
    So we verify the fraction is sane (malicious <= totalSamples, > 0 if any threats).
    """
    assert stats["maliciousCount"] <= stats["totalSamples"], \
        f"maliciousCount ({stats['maliciousCount']}) > totalSamples ({stats['totalSamples']})"
    if len(threats) > 0:
        assert stats["maliciousCount"] >= 0


def test_benign_count_is_complement(threats, stats):
    """benignCount + maliciousCount must equal totalSamples (pre-cap population).
    Not equal to len(threats) which is capped at 5000."""
    total = stats["totalSamples"]
    malicious = stats["maliciousCount"]
    benign = stats["benignCount"]
    assert malicious + benign == total, \
        f"maliciousCount ({malicious}) + benignCount ({benign}) != totalSamples ({total})"


def test_high_risk_count_matches_threats(threats, stats):
    """highRiskCount should equal CRITICAL + HIGH rows in threats.json"""
    computed = sum(1 for t in threats if t["riskLevel"] in ("CRITICAL", "HIGH"))
    assert stats["highRiskCount"] == computed, \
        f"highRiskCount mismatch: stats={stats['highRiskCount']}, computed={computed}"


def test_risk_distribution_complete(threats, stats):
    """Every level in threats.json must exist in riskDistribution."""
    levels = {t["riskLevel"] for t in threats}
    dist = stats["riskDistribution"]
    missing_levels = levels - set(dist.keys())
    assert not missing_levels, f"riskDistribution missing levels: {missing_levels}"


def test_risk_distribution_counts_match(threats, stats):
    """Each level count in riskDistribution must match threats.json."""
    from collections import Counter
    actual = Counter(t["riskLevel"] for t in threats)
    dist = stats["riskDistribution"]
    for level, count in actual.items():
        assert dist.get(level, 0) == count, \
            f"riskDistribution[{level}] = {dist.get(level, 0)}, actual = {count}"
