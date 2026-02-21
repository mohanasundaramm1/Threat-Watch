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
    DASHBOARD_DATA_DIR = os.path.join(REPO_ROOT, "..", "dashboard", "dashboard", "public", "data")

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
        
    X_full = build_features(Xdf)
    preds = booster.predict(X_full)
    Xdf["riskScore"] = preds
    # Flag MISP match if it's in the domain list or shares an IP
    Xdf["is_misp"] = Xdf.apply(lambda r: (r["registered_domain"] in misp_domains) or bool(r.get("matched_misp_ip", False)), axis=1)
    
    def get_misp_cat(r):
        rd = r["registered_domain"]
        if rd in misp_domains: return misp_domains[rd]
        if r.get("matched_misp_ip"): return "Network Infrastructure Overlap"
        return ""
        
    Xdf["misp_category"] = Xdf.apply(get_misp_cat, axis=1)
    
    # Format to frontend JSON structure
    out_rows = []
    for _, row in Xdf.iterrows():
        is_misp = row.get("is_misp", False)
        if is_misp:
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
            "source": "misp_osint" if is_misp else row.get("source", "unknown"),
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
    with open(os.path.join(DASHBOARD_DATA_DIR, "threats_recent.json"), "w") as f:
        json.dump(results_df.head(100).to_dict(orient="records"), f, indent=2)
    with open(os.path.join(DASHBOARD_DATA_DIR, "threats_high_risk.json"), "w") as f:
        json.dump(results_df[results_df["riskLevel"].isin(["CRITICAL", "HIGH"])].head(1000).to_dict(orient="records"), f, indent=2)
        
    # Generate Statistics
    stats = {
        "totalSamples": len(Xdf),
        "maliciousCount": len(Xdf[Xdf["riskScore"] >= 0.6]),
        "benignCount": len(Xdf[Xdf["riskScore"] < 0.6]),
        "riskDistribution": {
            "CRITICAL": len(results_df[results_df["riskLevel"] == "CRITICAL"]),
            "LOW": len(results_df[results_df["riskLevel"] == "LOW"]),
            "MEDIUM": len(results_df[results_df["riskLevel"] == "MEDIUM"]),
            "HIGH": len(results_df[results_df["riskLevel"] == "HIGH"])
        },
        "threatTypeDistribution": results_df["threatType"].value_counts().to_dict(),
        "sourceDistribution": results_df["source"].value_counts().to_dict(),
        "averageRiskScore": float(Xdf["riskScore"].mean()),
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
            
    print(f"Successfully exported {len(results_df)} threats and stats out of {len(Xdf)} total to {DASHBOARD_DATA_DIR}")

if __name__ == "__main__":
    ds = sys.argv[1] if len(sys.argv) > 1 else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    main(ds)
