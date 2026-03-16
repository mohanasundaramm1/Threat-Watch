"""
InvestigationAgent — the core autonomous threat investigation engine.

Gathers evidence from DNS, WHOIS, local threat DB, and ML model,
then orchestrates a complete domain investigation.
"""
from __future__ import annotations
import asyncio
import logging
import os
import socket
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import numpy as np

from models import (
    InvestigationResult, InvestigationFindings,
    DNSFindings, WHOISFindings, ThreatDBFindings, SHAPExplanation,
)

log = logging.getLogger(__name__)

# ── paths (container-aware) ──────────────────────────────────────────
REPO_ROOT = os.environ.get("DATA_ROOT", os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")))

SILVER_LABELS_DIR = os.path.join(REPO_ROOT, "silver", "labels_union")
DNS_GEO_DIR       = os.path.join(REPO_ROOT, "lookups", "dns_geo")
WHOIS_DIR         = os.path.join(REPO_ROOT, "lookups", "whois")
MODEL_DIR         = os.path.join(REPO_ROOT, "ml", "models", "registry")


# ── helpers ──────────────────────────────────────────────────────────
def _score_to_level(score: float) -> str:
    if score >= 0.90: return "CRITICAL"
    if score >= 0.60: return "HIGH"
    if score >= 0.20: return "MEDIUM"
    return "LOW"


def _reg_domain(domain: str) -> str:
    """Extract registered domain from a full domain string."""
    try:
        import tldextract
        ext = tldextract.extract(domain)
        return f"{ext.domain}.{ext.suffix}".lower() if ext.suffix else domain.lower()
    except Exception:
        return domain.lower()


class InvestigationAgent:
    """Autonomous domain investigation agent."""

    def __init__(self):
        self._model = None
        self._model_loaded = False

    # ── DNS ───────────────────────────────────────────────────────────
    def gather_dns(self, domain: str, timeout: float = 5.0) -> DNSFindings:
        """Resolve domain → IPs, ASN, country, IPv6."""
        findings = DNSFindings()
        try:
            import dns.resolver
            resolver = dns.resolver.Resolver()
            resolver.lifetime = timeout

            # A records
            ips = []
            try:
                answers = resolver.resolve(domain, "A")
                ips.extend([str(r) for r in answers])
            except Exception:
                pass

            # AAAA records
            try:
                answers = resolver.resolve(domain, "AAAA")
                ipv6 = [str(r) for r in answers]
                ips.extend(ipv6)
                if ipv6:
                    findings.has_ipv6 = True
            except Exception:
                pass

            findings.ips = ips
            findings.ip_count = len(ips)

            # GeoIP for first few IPs
            if ips:
                countries, asns, isps = [], [], []
                for ip in ips[:3]:  # limit to 3 lookups
                    geo = self._geoip_lookup(ip)
                    if geo.get("country"):
                        countries.append(geo["country"])
                    if geo.get("asn"):
                        asns.append(str(geo["asn"]))
                    if geo.get("isp"):
                        isps.append(geo["isp"])
                findings.countries = list(set(countries))
                findings.asns = list(set(asns))
                findings.isps = list(set(isps))

        except Exception as e:
            findings.error = str(e)
            log.warning("DNS gather failed for %s: %s", domain, e)

        return findings

    def _geoip_lookup(self, ip: str) -> dict:
        """Get country/ASN/ISP for an IP via ip-api.com."""
        import requests
        try:
            r = requests.get(
                f"http://ip-api.com/json/{ip}",
                params={"fields": "country,countryCode,isp,as,asname"},
                timeout=3,
            )
            if r.status_code == 200:
                data = r.json()
                return {
                    "country": data.get("countryCode", ""),
                    "asn": data.get("as", ""),
                    "isp": data.get("isp", ""),
                }
        except Exception:
            pass
        return {}

    # ── WHOIS ─────────────────────────────────────────────────────────
    def gather_whois(self, domain: str, timeout: float = 10.0) -> WHOISFindings:
        """Look up WHOIS registration data."""
        findings = WHOISFindings()
        try:
            import whois
            w = whois.whois(domain)

            findings.registrar = w.registrar or None

            # Created date
            created = w.creation_date
            if isinstance(created, list):
                created = created[0]
            if created:
                findings.created_date = str(created)
                age = (datetime.now() - created).days
                findings.age_days = max(age, 0)

            # Expiry date
            expiry = w.expiration_date
            if isinstance(expiry, list):
                expiry = expiry[0]
            if expiry:
                findings.expiry_date = str(expiry)

            # Status
            status = w.status
            if isinstance(status, list):
                findings.status = status[0] if status else None
            else:
                findings.status = str(status) if status else None

            # Name servers
            ns = w.name_servers
            if ns:
                if isinstance(ns, list):
                    findings.name_servers = [str(n).lower() for n in ns[:5]]
                else:
                    findings.name_servers = [str(ns).lower()]

        except Exception as e:
            findings.error = str(e)
            log.warning("WHOIS gather failed for %s: %s", domain, e)

        return findings

    # ── Threat DB ─────────────────────────────────────────────────────
    def check_threat_db(self, domain: str) -> ThreatDBFindings:
        """Check if domain exists in our local threat parquet data."""
        findings = ThreatDBFindings()
        reg = _reg_domain(domain)

        try:
            import glob
            parquet_files = sorted(
                glob.glob(os.path.join(SILVER_LABELS_DIR, "ingest_date=*", "*.parquet")),
                reverse=True
            )[:7]  # last 7 days

            if not parquet_files:
                return findings

            dfs = [pd.read_parquet(p) for p in parquet_files]
            labels = pd.concat(dfs, ignore_index=True)

            # Search by domain match
            matches = labels[
                labels["domain"].astype(str).str.lower().str.contains(reg, na=False)
            ]

            if len(matches) > 0:
                findings.found = True
                findings.sources = list(matches["source"].dropna().unique())
                findings.labels = list(matches["label"].dropna().unique())
                if "first_seen" in matches.columns:
                    first = matches["first_seen"].min()
                    findings.first_seen = str(first) if pd.notna(first) else None

        except Exception as e:
            log.warning("Threat DB check failed for %s: %s", domain, e)

        return findings

    # ── ML Scoring + SHAP ─────────────────────────────────────────────
    def score_domain(self, domain: str, dns: DNSFindings, whois: WHOISFindings
                     ) -> tuple[Optional[float], SHAPExplanation]:
        """Score domain with LightGBM and compute SHAP feature contributions."""
        shap_result = SHAPExplanation()
        score = None

        try:
            import joblib
            import lightgbm as lgb

            # Load model
            model_path = os.path.join(MODEL_DIR, "model.pkl")
            if not os.path.exists(model_path):
                log.warning("Model not found at %s", model_path)
                return None, shap_result

            model = joblib.load(model_path)

            # Build a minimal feature vector matching retrain_model.py
            features = self._build_feature_dict(domain, dns, whois)
            feature_names = list(features.keys())
            X = np.array([list(features.values())])

            # Predict
            if hasattr(model, "predict_proba"):
                score = float(model.predict_proba(X)[:, 1][0])
            else:
                score = float(model.predict(X)[0])

            # SHAP
            try:
                import shap
                explainer = shap.TreeExplainer(model)
                shap_values = explainer.shap_values(X)

                # For binary classification, shap_values may be a list
                if isinstance(shap_values, list):
                    sv = shap_values[1][0]  # class 1 (malicious)
                else:
                    sv = shap_values[0]

                contributions = dict(zip(feature_names, sv))
                # Sort by absolute value, top 10
                sorted_contribs = sorted(
                    contributions.items(), key=lambda x: abs(x[1]), reverse=True
                )[:10]

                shap_result.feature_contributions = {k: round(float(v), 4) for k, v in sorted_contribs}

                # Human-readable top risk factors
                risk_factors = []
                for name, val in sorted_contribs[:5]:
                    direction = "increases" if val > 0 else "decreases"
                    readable = name.replace("_", " ").title()
                    risk_factors.append(f"{readable} {direction} risk ({val:+.3f})")
                shap_result.top_risk_factors = risk_factors

            except Exception as e:
                log.warning("SHAP computation failed: %s", e)

        except Exception as e:
            log.warning("ML scoring failed for %s: %s", domain, e)

        return score, shap_result

    def _build_feature_dict(self, domain: str, dns: DNSFindings, whois: WHOISFindings) -> dict:
        """Build a feature dictionary for a single domain."""
        import math

        # Character-level features
        domain_len = len(domain)
        dot_count = domain.count(".")
        hyphen_count = domain.count("-")
        digit_count = sum(c.isdigit() for c in domain)
        digit_ratio = digit_count / max(domain_len, 1)

        # Entropy
        freq = {}
        for c in domain:
            freq[c] = freq.get(c, 0) + 1
        entropy = -sum((f / domain_len) * math.log2(f / domain_len) for f in freq.values()) if domain_len > 0 else 0

        # Subdomain depth
        parts = domain.split(".")
        subdomain_depth = max(len(parts) - 2, 0)

        # DNS features
        num_unique_ips = dns.ip_count
        has_ipv6 = int(dns.has_ipv6)
        num_countries = len(dns.countries)
        num_asns = len(dns.asns)

        # WHOIS features
        domain_age_days = whois.age_days if whois.age_days is not None else -1

        return {
            "domain_length": domain_len,
            "dot_count": dot_count,
            "hyphen_count": hyphen_count,
            "digit_count": digit_count,
            "digit_ratio": digit_ratio,
            "entropy": round(entropy, 4),
            "subdomain_depth": subdomain_depth,
            "num_unique_ips": num_unique_ips,
            "has_ipv6": has_ipv6,
            "num_countries": num_countries,
            "num_asns": num_asns,
            "domain_age_days": domain_age_days,
        }

    # ── Full Investigation ────────────────────────────────────────────
    def investigate(self, domain: str) -> InvestigationResult:
        """Run a complete autonomous investigation on a domain."""
        start = time.time()
        inv_id = uuid.uuid4().hex[:12]

        log.info("Starting investigation %s for %s", inv_id, domain)

        # Step 1: Gather evidence (DNS + WHOIS can run together conceptually)
        dns = self.gather_dns(domain)
        whois = self.gather_whois(domain)
        threat_db = self.check_threat_db(domain)

        # Step 2: ML scoring + SHAP
        risk_score, shap = self.score_domain(domain, dns, whois)
        risk_level = _score_to_level(risk_score) if risk_score is not None else None

        # If domain is in threat DB, boost confidence
        if threat_db.found and risk_score is not None and risk_score < 0.5:
            risk_score = max(risk_score, 0.65)
            risk_level = _score_to_level(risk_score)

        findings = InvestigationFindings(
            dns=dns,
            whois=whois,
            threat_db=threat_db,
            shap=shap,
        )

        elapsed = round(time.time() - start, 2)

        return InvestigationResult(
            id=inv_id,
            domain=domain,
            risk_score=risk_score,
            risk_level=risk_level,
            findings=findings,
            investigated_at=datetime.now(timezone.utc).isoformat(),
            latency_seconds=elapsed,
        )
