"""
Critical Test: Enrichment Cache Integrity
Verifies the DNS/Geo and WHOIS persistent caches (lookups/dns_geo_cache.parquet,
lookups/whois_cache.parquet) are well-formed, and reports current coverage —
the % of domains in labels_union that have at least one resolved cache row.
This gives a concrete, re-runnable number for the "enrichment coverage" figure
cited in FINAL_REPORT.md, instead of a stale hand-copied percentage.
"""
import os
import glob
import pandas as pd
import pytest

THREAT_INTEL_ROOT = os.path.join(os.path.dirname(__file__), "..")
LOOKUPS = os.path.join(THREAT_INTEL_ROOT, "lookups")
LABELS_UNION = os.path.join(THREAT_INTEL_ROOT, "silver", "labels_union")


def _all_domains():
    parts = glob.glob(os.path.join(LABELS_UNION, "ingest_date=*", "labels_union.parquet"))
    if not parts:
        pytest.skip("no labels_union partitions found — run the ingestion pipeline first")
    dfs = [pd.read_parquet(p, columns=["domain"]) for p in parts]
    return pd.concat(dfs, ignore_index=True)["domain"].dropna().str.lower().unique()


def test_dns_geo_cache_schema():
    path = os.path.join(LOOKUPS, "dns_geo_cache.parquet")
    if not os.path.exists(path):
        pytest.skip("dns_geo_cache.parquet not found")
    df = pd.read_parquet(path)
    required = {"puny_domain", "ip", "family", "country", "asn"}
    assert required.issubset(df.columns), f"dns_geo_cache missing columns: {required - set(df.columns)}"
    assert not df["puny_domain"].isna().all(), "dns_geo_cache has no resolved domains"


def test_whois_cache_schema():
    path = os.path.join(LOOKUPS, "whois_cache.parquet")
    if not os.path.exists(path):
        pytest.skip("whois_cache.parquet not found")
    df = pd.read_parquet(path)
    assert "domain" in df.columns, "whois_cache missing 'domain' column"
    assert not df["domain"].isna().all(), "whois_cache has no domains"


def test_report_enrichment_coverage():
    """Not a strict assertion — prints current DNS/WHOIS coverage % for the report."""
    domains = set(_all_domains())

    dns_path = os.path.join(LOOKUPS, "dns_geo_cache.parquet")
    whois_path = os.path.join(LOOKUPS, "whois_cache.parquet")

    dns_covered = 0
    if os.path.exists(dns_path):
        dns_df = pd.read_parquet(dns_path, columns=["puny_domain"])
        dns_covered = len(domains & set(dns_df["puny_domain"].str.lower().unique()))

    whois_covered = 0
    if os.path.exists(whois_path):
        whois_df = pd.read_parquet(whois_path, columns=["domain"])
        whois_covered = len(domains & set(whois_df["domain"].str.lower().unique()))

    total = len(domains) or 1
    print(
        f"\n[enrichment coverage] total_domains={total} "
        f"dns_geo_coverage={dns_covered/total:.1%} "
        f"whois_coverage={whois_covered/total:.1%}"
    )
    assert total > 0
