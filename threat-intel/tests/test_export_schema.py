"""
Critical Test 1: Export Schema Validation
Verifies that threats.json, statistics.json, and model_meta.json have the
required fields and correct types — catching regressions before they reach the UI.
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


class TestThreatsSchema:
    REQUIRED_FIELDS = {"domain", "url", "firstSeen", "label", "source",
                       "riskScore", "riskLevel", "threatType", "country",
                       "registrar", "domainAgeDays"}

    def test_threats_is_list(self):
        data = _load("threats.json")
        assert isinstance(data, list), "threats.json must be a JSON array"

    def test_threats_not_empty(self):
        data = _load("threats.json")
        assert len(data) > 0, "threats.json must contain at least one record"

    def test_threats_fields_present(self):
        data = _load("threats.json")
        first = data[0]
        missing = self.REQUIRED_FIELDS - set(first.keys())
        assert not missing, f"threats.json missing fields: {missing}"

    def test_risk_score_range(self):
        data = _load("threats.json")
        for row in data[:50]:  # spot-check first 50
            score = row["riskScore"]
            assert 0.0 <= score <= 1.0, f"riskScore out of range: {score}"

    def test_risk_level_values(self):
        data = _load("threats.json")
        valid_levels = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
        for row in data[:50]:
            assert row["riskLevel"] in valid_levels, \
                f"Unknown riskLevel: {row['riskLevel']}"


class TestStatisticsSchema:
    REQUIRED_FIELDS = {"totalSamples", "maliciousCount", "benignCount",
                       "riskDistribution", "highRiskCount", "averageRiskScore"}

    def test_statistics_fields_present(self):
        stats = _load("statistics.json")
        missing = self.REQUIRED_FIELDS - set(stats.keys())
        assert not missing, f"statistics.json missing fields: {missing}"

    def test_counts_add_up(self):
        """maliciousCount + benignCount must equal totalSamples (using capped result set)."""
        stats = _load("statistics.json")
        assert stats["maliciousCount"] + stats["benignCount"] == len(_load("threats.json")), \
            "maliciousCount + benignCount must equal total exported records"

    def test_risk_distribution_sums_to_total(self):
        stats = _load("statistics.json")
        dist = stats["riskDistribution"]
        total = sum(dist.values())
        assert total == len(_load("threats.json")), \
            f"riskDistribution sum ({total}) != threats.json row count"

    def test_average_risk_score_within_range(self):
        stats = _load("statistics.json")
        avg = stats["averageRiskScore"]
        assert 0.0 <= avg <= 1.0, f"averageRiskScore out of range: {avg}"


class TestModelMetaSchema:
    REQUIRED_FIELDS = {"model_file", "trained_at", "n_rows", "metrics"}

    def test_model_meta_fields_present(self):
        meta = _load("model_meta.json")
        missing = self.REQUIRED_FIELDS - set(meta.keys())
        assert not missing, f"model_meta.json missing fields: {missing}"

    def test_metrics_has_roc_auc(self):
        meta = _load("model_meta.json")
        assert "roc_auc" in meta.get("metrics", {}), \
            "model_meta.json metrics must include roc_auc"
