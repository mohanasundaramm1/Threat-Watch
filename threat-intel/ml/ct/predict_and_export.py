# ml/ct/predict_and_export.py
import os, json, sys, math
import traceback
from datetime import datetime, timezone
import pandas as pd
import numpy as np

# Re-use the feature building logic
from retrain_model import build_features, df_from_parquets, reg_domain

import lightgbm as lgb
import joblib

THIS_DIR = os.path.dirname(__file__)
ML_DIR = os.path.abspath(os.path.join(THIS_DIR, ".."))
REPO_ROOT = os.path.abspath(os.path.join(ML_DIR, ".."))

SILVER_LABELS_DIR = os.path.join(REPO_ROOT, "silver", "labels_union")
DNS_GEO_DIR       = os.path.join(REPO_ROOT, "lookups", "dns_geo")
WHOIS_DIR         = os.path.join(REPO_ROOT, "lookups", "whois")
MISP_OSINT_DIR    = os.path.join(REPO_ROOT, "silver", "misp_osint")
MODEL_DIR         = os.path.join(ML_DIR, "models", "registry")
# In docker-compose we mounted dashboard data up two directories
# So it's accessible via /opt/airflow/dashboard_data inside the container, or we find it relative to REPO_ROOT
if os.path.exists("/opt/airflow/dashboard_data"):
    DASHBOARD_DATA_DIR = "/opt/airflow/dashboard_data"
else:
    DASHBOARD_DATA_DIR = os.path.join(REPO_ROOT, "dashboard", "public", "data")

os.makedirs(DASHBOARD_DATA_DIR, exist_ok=True)

def score_to_level(score):
    if score >= 0.90: return "CRITICAL"
    if score >= 0.60: return "HIGH"
    if score >= 0.20: return "MEDIUM"
    return "LOW"

def infer_threat_type(source):
    s = source.lower()
    if "urlhaus" in s: return "malware"
    if "openphish" in s: return "phishing"
    if "misp" in s: return "malware"
    return "suspicious"

def main(date_str):
    print(f"Running prediction and export for {date_str}...")
    
    # Load Data
    label_path = os.path.join(SILVER_LABELS_DIR, f"ingest_date={date_str}")
    dns_path = os.path.join(DNS_GEO_DIR, f"ingest_date={date_str}")
    whois_path = os.path.join(WHOIS_DIR, f"ingest_date={date_str}")
    
    labels = df_from_parquets([label_path])
    if labels.empty:
        print(f"No labels found for {date_str}. Exiting.")
        return
        
    labels["registered_domain"] = labels["domain"].map(reg_domain)
    labels = labels[labels["registered_domain"].astype(bool)].copy()
    
    # Load MISP data
    misp_path = os.path.join(MISP_OSINT_DIR, f"ingest_date={date_str}")
    misp_df = df_from_parquets([misp_path])
    misp_domains = {}
    misp_ips = set()
    if not misp_df.empty:
        # Collect domains
        misp_subset = misp_df[misp_df["type"].isin(["domain", "hostname", "url"])].copy()
        if not misp_subset.empty:
            misp_subset["registered_domain"] = misp_subset["indicator"].astype(str).apply(reg_domain)
            for _, row in misp_subset.iterrows():
                rd = row["registered_domain"]
                if rd:
                    misp_domains[rd] = row.get("category", "misp_osint")
                    
        # Collect IP infrastructure
        ip_subset = misp_df[misp_df["type"].isin(["ip-src", "ip-dst"])].copy()
        if not ip_subset.empty:
            misp_ips = set(ip_subset["indicator"].astype(str).str.strip())
    
    last_seen = labels.groupby("registered_domain")["first_seen"].max().reset_index()
    
    # Lookups
    dns = df_from_parquets([dns_path])
    agg = pd.DataFrame(columns=["registered_domain", "matched_misp_ip"])
    if not dns.empty:
        dns = dns.rename(columns={"puny_domain": "registered_domain"})
        
        # Check IP overlap before grouping
        dns["is_misp_ip"] = dns["ip"].astype(str).isin(misp_ips)
        misp_ip_flags = dns.groupby("registered_domain")["is_misp_ip"].max().reset_index(name="matched_misp_ip")
        
        agg = dns.groupby("registered_domain").agg(
            num_unique_ips=("ip", "nunique"),
            has_ipv6=("family", lambda x: int((pd.Series(x) == 6).any())),
            num_countries=("country", "nunique"),
            num_asns=("asn", "nunique"),
            sample_asn=("asn", "first"),
            sample_isp=("isp", "first"),
            sample_country=("country", "first"),
        ).reset_index()
        agg = agg.merge(misp_ip_flags, on="registered_domain", how="left")

    whois = df_from_parquets([whois_path])
    if not whois.empty:
        wcols = ["domain", "registrar", "status", "created", "expires", "error"]
        whois = whois[[c for c in wcols if c in whois.columns]].copy()
        whois["registered_domain"] = whois["domain"].astype(str).str.lower().str.strip()
        NOW_UTC = datetime.now(timezone.utc)
        whois["created"] = pd.to_datetime(whois.get("created"), utc=True, errors="coerce")
        whois["expires"] = pd.to_datetime(whois.get("expires"), utc=True, errors="coerce")
        whois["age_days"] = (NOW_UTC - whois["created"]).dt.total_seconds() / 86400.0
        whois["days_to_expiry"] = (whois["expires"] - NOW_UTC).dt.total_seconds() / 86400.0
        whois["created_isnull"] = whois["created"].isna().astype(int)
        whois["expires_isnull"] = whois["expires"].isna().astype(int)
        whois["has_error"] = whois.get("error").notna().astype(int)
    else:
        whois = pd.DataFrame(columns=["registered_domain"])
        
    Xdf = labels.drop_duplicates("registered_domain").merge(agg, on="registered_domain", how="left").merge(whois, on="registered_domain", how="left")
    
    # Load Model
    meta_path = os.path.join(MODEL_DIR, "latest_meta.json")
    if not os.path.exists(meta_path):
        meta_path = os.path.join(MODEL_DIR, "week5_meta.json") # fallback
        
    try:
        with open(meta_path, "r") as f:
            meta = json.load(f)
        model_name = meta.get("model_file", "week5_lgbm_full.txt")
        model_path = os.path.join(MODEL_DIR, model_name)
        booster = lgb.Booster(model_file=model_path)
    except Exception as e:
        print(f"Failed to load latest model: {e}")
        traceback.print_exc()
        return
        
    # Flag MISP match with the same column name used during training (is_in_misp)
    Xdf["is_in_misp"] = Xdf.apply(
        lambda r: int((r["registered_domain"] in misp_domains) or bool(r.get("matched_misp_ip", False))),
        axis=1
    )
    
    X_full = build_features(Xdf)
    preds = booster.predict(X_full)
    Xdf["riskScore"] = preds
    
    def get_misp_cat(r):
        rd = r["registered_domain"]
        if rd in misp_domains: return misp_domains[rd]
        if r.get("matched_misp_ip"): return "Network Infrastructure Overlap"
        return ""
        
    Xdf["misp_category"] = Xdf.apply(get_misp_cat, axis=1)
    
    # Format to frontend JSON structure
    out_rows = []
    for _, row in Xdf.iterrows():
        is_in_misp = bool(row.get("is_in_misp", 0))
        if is_in_misp:
            score = 1.00
            risk_level = "CRITICAL"
            threat_type = str(row.get("misp_category", "misp_osint")).lower()
            if not threat_type or threat_type == "nan": threat_type = "misp_osint"
        else:
            score = float(row["riskScore"])
            risk_level = score_to_level(score)
            threat_type = infer_threat_type(row.get("source", ""))
            
        out_rows.append({
            "domain": row.get("domain_x", row.get("domain")),
            "url": row.get("url", ""),
            "firstSeen": str(row.get("first_seen", "")),
            "label": row.get("label", "unknown"),
            "source": "misp_osint" if is_in_misp else row.get("source", "unknown"),
            "riskScore": round(score, 4),
            "riskLevel": risk_level,
            "threatType": threat_type,
            "country": row.get("sample_country", "Unknown") if pd.notna(row.get("sample_country")) else "Unknown",
            "registrar": row.get("registrar", "Unknown") if pd.notna(row.get("registrar")) else "Unknown",
            "domainAgeDays": int(row.get("age_days", -1)) if pd.notna(row.get("age_days")) else -1
        })
        
    results_df = pd.DataFrame(out_rows)
    results_df = results_df.sort_values("riskScore", ascending=False).head(5000)
    
    # Write threats.json
    threats_path = os.path.join(DASHBOARD_DATA_DIR, "threats.json")
    with open(threats_path, "w") as f:
        json.dump(results_df.to_dict(orient="records"), f, indent=2)
        
    # Write threats_recent.json and threats_high_risk.json (Top 100 for recent, top 1000 for high risk)
    recent_df = results_df.sort_values("firstSeen", ascending=False).head(100)
    with open(os.path.join(DASHBOARD_DATA_DIR, "threats_recent.json"), "w") as f:
        json.dump(recent_df.to_dict(orient="records"), f, indent=2)
    with open(os.path.join(DASHBOARD_DATA_DIR, "threats_high_risk.json"), "w") as f:
        json.dump(results_df[results_df["riskLevel"].isin(["CRITICAL", "HIGH"])].head(1000).to_dict(orient="records"), f, indent=2)
        
    # Generate Statistics
    stats = {
        "totalSamples": len(Xdf),
        "maliciousCount": len(results_df[results_df["riskScore"] >= 0.6]),
        "benignCount": len(results_df) - len(results_df[results_df["riskScore"] >= 0.6]),
        "riskDistribution": {
            "CRITICAL": len(results_df[results_df["riskLevel"] == "CRITICAL"]),
            "LOW": len(results_df[results_df["riskLevel"] == "LOW"]),
            "MEDIUM": len(results_df[results_df["riskLevel"] == "MEDIUM"]),
            "HIGH": len(results_df[results_df["riskLevel"] == "HIGH"])
        },
        "threatTypeDistribution": results_df["threatType"].value_counts().to_dict(),
        "sourceDistribution": results_df["source"].value_counts().to_dict(),
        "averageRiskScore": float(results_df["riskScore"].mean()),
        "highRiskCount": len(results_df[results_df["riskLevel"].isin(["CRITICAL", "HIGH"])]),
        "dateRange": {
            "earliest": str(Xdf["first_seen"].min()),
            "latest": str(Xdf["first_seen"].max())
        } if "first_seen" in Xdf.columns else {},
        "topCountries": results_df[results_df["country"] != "Unknown"]["country"].value_counts().head(10).to_dict(),
        "topRegistrars": results_df[results_df["registrar"] != "Unknown"]["registrar"].value_counts().head(10).to_dict()
    }
    
    with open(os.path.join(DASHBOARD_DATA_DIR, "statistics.json"), "w") as f:
        json.dump(stats, f, indent=2)
        
    import shutil
    if os.path.exists(meta_path):
        target_meta_path = os.path.join(DASHBOARD_DATA_DIR, "model_meta.json")
        try:
            shutil.copy2(meta_path, target_meta_path)
            print(f"Copied {meta_path} to {target_meta_path}")
        except Exception as e:
            print(f"Failed to copy model metadata: {e}")
            
    # --- Drift Detection ---
    drift_path = os.path.join(DASHBOARD_DATA_DIR, "drift.json")
    prev_stats_path = os.path.join(DASHBOARD_DATA_DIR, "statistics_prev.json")
    # Rotate: save current stats as "prev" for next run comparison
    if os.path.exists(os.path.join(DASHBOARD_DATA_DIR, "statistics.json")):
        try:
            import shutil as _shutil
            _shutil.copy2(os.path.join(DASHBOARD_DATA_DIR, "statistics.json"), prev_stats_path)
        except Exception as _e:
            print(f"[drift] Could not rotate stats: {_e}")

    prev_stats = {}
    if os.path.exists(prev_stats_path):
        try:
            with open(prev_stats_path) as _f:
                prev_stats = json.load(_f)
        except Exception:
            prev_stats = {}

    def _safe_delta(current, prev, key):
        if key in prev and prev[key] is not None and current.get(key) is not None:
            return round(float(current[key]) - float(prev[key]), 6)
        return None

    curr_malicious_ratio = (stats["maliciousCount"] / stats["totalSamples"]) if stats["totalSamples"] else 0
    prev_malicious_ratio = (prev_stats["maliciousCount"] / prev_stats["totalSamples"]) if prev_stats.get("totalSamples") else None

    # Top ASN drift: compare top-5 ASNs today vs prev (if available)
    curr_top_asn = results_df["source"].value_counts().head(5).to_dict()  # proxy until ASN col available
    if "sample_asn" in results_df.columns:
        curr_top_asn = results_df["sample_asn"].dropna().value_counts().head(5).to_dict()

    drift = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "date": date_str,
        "total_samples": stats["totalSamples"],
        "malicious_ratio": round(curr_malicious_ratio, 4),
        "malicious_ratio_delta": round(curr_malicious_ratio - prev_malicious_ratio, 4) if prev_malicious_ratio is not None else None,
        "avg_risk_score": stats["averageRiskScore"],
        "avg_risk_score_delta": _safe_delta(stats, prev_stats, "averageRiskScore"),
        "high_risk_count": stats["highRiskCount"],
        "high_risk_count_delta": _safe_delta(stats, prev_stats, "highRiskCount"),
        "risk_distribution": stats["riskDistribution"],
        "top_asns": curr_top_asn,
        "alert": None,
    }

    # Simple alert thresholds
    alerts = []
    if drift["malicious_ratio_delta"] is not None and abs(drift["malicious_ratio_delta"]) > 0.10:
        alerts.append(f"Malicious ratio shifted by {drift['malicious_ratio_delta']:+.1%}")
    if drift["avg_risk_score_delta"] is not None and abs(drift["avg_risk_score_delta"]) > 0.05:
        alerts.append(f"Average risk score shifted by {drift['avg_risk_score_delta']:+.4f}")
    if alerts:
        drift["alert"] = " | ".join(alerts)
        print(f"[DRIFT ALERT] {drift['alert']}")

    with open(drift_path, "w") as f:
        json.dump(drift, f, indent=2)
    print(f"Drift manifest written to {drift_path}")

    print(f"Successfully exported {len(results_df)} threats and stats out of {len(Xdf)} total to {DASHBOARD_DATA_DIR}")

if __name__ == "__main__":
    ds = sys.argv[1] if len(sys.argv) > 1 else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    main(ds)
