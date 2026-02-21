# ml/ct/retrain_model.py
import os, glob, math, json, sys
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import tldextract
from collections import Counter

from sklearn.feature_extraction.text import HashingVectorizer, FeatureHasher
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, average_precision_score, roc_curve
from sklearn.model_selection import train_test_split
from scipy.sparse import csr_matrix, hstack
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
TMP_DIR           = os.path.join(ML_DIR, "tmp")

os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)

NOW_UTC = datetime.now(timezone.utc)

def reg_domain(domain: str) -> str:
    if not isinstance(domain, str) or not domain: return ""
    ext = tldextract.extract(domain)
    reg = getattr(ext, "registered_domain", None) or getattr(ext, "top_domain_under_public_suffix", None) or ""
    return reg.lower().strip()

def shannon_entropy(s: str) -> float:
    if not s: return 0.0
    c = Counter(s)
    n = len(s)
    return -sum((v / n) * math.log2(v / n) for v in c.values())

def basic_string_feats(dom: str) -> dict:
    d = dom or ""
    feats = {"len": len(d), "digits": sum(ch.isdigit() for ch in d), "hyphens": d.count("-"), "dots": d.count(".")}
    feats["digit_ratio"] = feats["digits"] / (feats["len"] + 1e-6)
    feats["hyphen_ratio"] = feats["hyphens"] / (feats["len"] + 1e-6)
    feats["entropy"] = shannon_entropy(d)
    feats["xn_punycode"] = int("xn--" in d)
    parts = d.split(".")
    feats["labels"] = len([p for p in parts if p])
    feats["tld_len"] = len(parts[-1]) if parts else 0
    return feats

def df_from_parquets(paths):
    files = []
    for p in paths:
        if os.path.isdir(p):
            files.extend(glob.glob(os.path.join(p, "*.parquet")))
        elif os.path.isfile(p):
            files.append(p)
    dfs = []
    for f in sorted(files):
        try: dfs.append(pd.read_parquet(f))
        except Exception as e: print(f"[warn] failed {f}: {e}")
    if not dfs: return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

def build_features(Xdf):
    domains = Xdf["registered_domain"].fillna("")
    char_vect = HashingVectorizer(analyzer="char", ngram_range=(3, 5), n_features=4096, lowercase=True)
    X_char = char_vect.transform(domains.tolist())

    str_feats = ["len", "digits", "hyphens", "dots", "digit_ratio", "hyphen_ratio", "entropy", "xn_punycode", "labels", "tld_len"]
    S = np.vstack([[basic_string_feats(d).get(k, 0) for k in str_feats] for d in domains.tolist()])
    X_string = csr_matrix(S)

    dns_cols = ["num_unique_ips", "has_ipv6", "num_countries", "num_asns"]
    for c in dns_cols: 
        if c not in Xdf.columns: Xdf[c] = 0
    X_dns = csr_matrix(Xdf[dns_cols].fillna(0).to_numpy(dtype=float))

    def cat_row(r):
        d = {}
        for f in ["sample_asn", "sample_isp", "sample_country", "registrar", "status"]:
            v = r.get(f)
            if v is not None and not (isinstance(v, float) and math.isnan(v)):
                d[f"{f}={str(v).strip()}"] = 1
        return d
    hasher = FeatureHasher(n_features=256, input_type="dict")
    X_cat = hasher.transform([cat_row(r) for r in Xdf.to_dict(orient="records")])

    whois_cols = ["age_days", "days_to_expiry", "created_isnull", "expires_isnull", "has_error", "is_in_misp"]
    for c in whois_cols: 
        if c not in Xdf.columns: Xdf[c] = 0
    X_whois = csr_matrix(Xdf[whois_cols].fillna(0).to_numpy(dtype=float))

    X_full = hstack([X_char, X_string, X_dns, X_cat, X_whois]).tocsr()
    return X_full

def evaluate_model(name, clf, Xtr, ytr, Xte, yte):
    clf.fit(Xtr, ytr)
    p_te = clf.predict_proba(Xte)[:, 1]
    roc = roc_auc_score(yte, p_te)
    pr = average_precision_score(yte, p_te)
    fpr, tpr, thr = roc_curve(yte, p_te)
    idx = np.searchsorted(fpr, 0.01, side="right") - 1 if (fpr >= 0.01).any() else len(tpr)-1
    r_at_1pct = float(tpr[max(idx, 0)])
    print(f"[{name}] ROC-AUC={roc:.4f}  PR-AUC={pr:.4f}  Recall@1%FPR={r_at_1pct:.3f}")
    return clf, {"roc_auc": roc, "pr_auc": pr, "recall_at_1pct": r_at_1pct}

def get_recent_partitions(base_dir, days_back):
    partitions = []
    for d in range(days_back):
        date_str = (NOW_UTC - timedelta(days=d)).strftime("%Y-%m-%d")
        path = os.path.join(base_dir, f"ingest_date={date_str}")
        if os.path.exists(path):
            partitions.append(path)
    return partitions

def main(days_back=14):
    print(f"Retraining model using data from the last {days_back} days...")
    labels_paths = get_recent_partitions(SILVER_LABELS_DIR, days_back)
    dns_paths = get_recent_partitions(DNS_GEO_DIR, days_back)
    whois_paths = get_recent_partitions(WHOIS_DIR, days_back)
    misp_paths = get_recent_partitions(MISP_OSINT_DIR, days_back)
    
    labels = df_from_parquets(labels_paths)
    if labels.empty:
        raise ValueError("No label data found to train on.")
    
    labels["registered_domain"] = labels["domain"].map(reg_domain)
    labels = labels[labels["registered_domain"].astype(bool)].copy()
    labels["label"] = np.where(labels["source"] == "benign_seed", 0, 1)
    labels = labels[labels["label"].isin([0, 1])].copy()
    
    last_seen = labels.groupby("registered_domain")["first_seen"].max().reset_index()
    
    dns = df_from_parquets(dns_paths)
    if not dns.empty:
        dns = dns.rename(columns={"puny_domain": "registered_domain"})
        agg = dns.groupby("registered_domain").agg(
            num_unique_ips=("ip", "nunique"),
            has_ipv6=("family", lambda x: int((pd.Series(x) == 6).any())),
            num_countries=("country", "nunique"),
            num_asns=("asn", "nunique"),
            sample_asn=("asn", "first"),
            sample_isp=("isp", "first"),
            sample_country=("country", "first"),
        ).reset_index()
    else:
        agg = pd.DataFrame(columns=["registered_domain"])
        
    whois = df_from_parquets(whois_paths)
    if not whois.empty:
        wcols = ["domain", "registrar", "status", "created", "expires", "error"]
        whois = whois[[c for c in wcols if c in whois.columns]].copy()
        whois["registered_domain"] = whois["domain"].astype(str).str.lower().str.strip()
        whois["created"] = pd.to_datetime(whois.get("created"), utc=True, errors="coerce")
        whois["expires"] = pd.to_datetime(whois.get("expires"), utc=True, errors="coerce")
        whois["age_days"] = (NOW_UTC - whois["created"]).dt.total_seconds() / 86400.0
        whois["days_to_expiry"] = (whois["expires"] - NOW_UTC).dt.total_seconds() / 86400.0
        whois["created_isnull"] = whois["created"].isna().astype(int)
        whois["expires_isnull"] = whois["expires"].isna().astype(int)
        whois["has_error"] = whois.get("error").notna().astype(int)
    else:
        whois = pd.DataFrame(columns=["registered_domain"])
        
    misp_df = df_from_parquets(misp_paths)
    if not misp_df.empty:
        misp_subset = misp_df[misp_df["type"].isin(["domain", "hostname", "url"])].copy()
        if not misp_subset.empty:
            misp_subset["registered_domain"] = misp_subset["indicator"].astype(str).apply(reg_domain)
            misp_rd = set(misp_subset[misp_subset["registered_domain"] != ""]["registered_domain"])
        else:
            misp_rd = set()
    else:
        misp_rd = set()
        
    Xdf = labels[["registered_domain", "label"]].drop_duplicates("registered_domain").merge(last_seen, on="registered_domain", how="left").merge(agg, on="registered_domain", how="left").merge(whois, on="registered_domain", how="left")
    Xdf["is_in_misp"] = Xdf["registered_domain"].map(lambda rd: int(rd in misp_rd))
    
    print(f"[info] rows: {len(Xdf)} | pos: {(Xdf['label']==1).sum()} | neg: {(Xdf['label']==0).sum()}")
    
    X_full = build_features(Xdf)
    y = Xdf["label"].astype(int).to_numpy()
    
    Xtr, Xte, y_train, y_test = train_test_split(X_full, y, test_size=0.25, stratify=y, random_state=42)
    
    lgbm = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.05, num_leaves=63, objective="binary", class_weight="balanced", n_jobs=-1)
    lgbm, metrics = evaluate_model("LightGBM_Retrained", lgbm, Xtr, y_train, Xte, y_test)
    
    model_name = "latest_lgbm.txt"
    lgbm.booster_.save_model(os.path.join(MODEL_DIR, model_name))
    
    meta = {
        "created_utc": NOW_UTC.isoformat(),
        "n_rows": int(len(Xdf)),
        "n_pos": int((y == 1).sum()),
        "n_neg": int((y == 0).sum()),
        "metrics": metrics,
        "model_file": model_name
    }
    with open(os.path.join(MODEL_DIR, "latest_meta.json"), "w") as f:
        json.dump(meta, f, indent=2)
    print(f"Model and metadata saved to {MODEL_DIR}")

if __name__ == "__main__":
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 14
    main(days)
