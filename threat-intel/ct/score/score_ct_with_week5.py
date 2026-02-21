# ct/score/score_ct_with_week5.py

import os
import glob
import math
import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix, hstack
from sklearn.feature_extraction.text import HashingVectorizer, FeatureHasher

# Optional deps
try:
    import joblib
except ImportError:
    joblib = None

try:
    import lightgbm as lgb
except ImportError:
    lgb = None

# ---------- paths ----------

THIS_DIR  = os.path.dirname(__file__)                      # .../ct/score
CT_DIR    = os.path.abspath(os.path.join(THIS_DIR, ".."))  # .../ct
REPO_ROOT = os.path.abspath(os.path.join(CT_DIR, ".."))    # .../threat-intel

DATA_DIR = os.path.join(CT_DIR, "data")

ENRICHED_DIR_DEFAULT = os.path.join(DATA_DIR, "enriched")
SCORED_DIR_DEFAULT   = os.path.join(DATA_DIR, "scored")

MODEL_REG_DIR = os.path.join(REPO_ROOT, "ml", "models", "registry")

ENRICHED_DIR = os.getenv("CT_ENRICHED_DIR", ENRICHED_DIR_DEFAULT)
SCORED_DIR   = os.getenv("CT_SCORED_DIR", SCORED_DIR_DEFAULT)

os.makedirs(SCORED_DIR, exist_ok=True)

MAX_ENRICHED_FILES = int(os.getenv("MAX_ENRICHED_FILES", "50"))

NOW_UTC = datetime.now(timezone.utc)

# ---------- helpers copied from week5 training ----------

from collections import Counter
import tldextract


def reg_domain(domain: str) -> str:
    """Normalize to registered domain (fallback if needed)."""
    if not isinstance(domain, str) or not domain:
        return ""
    ext = tldextract.extract(domain)
    reg = getattr(ext, "registered_domain", None) or getattr(
        ext, "top_domain_under_public_suffix", None
    ) or ""
    return reg.lower().strip()


def shannon_entropy(s: str) -> float:
    if not s:
        return 0.0
    c = Counter(s)
    n = len(s)
    return -sum((v / n) * math.log2(v / n) for v in c.values())


def basic_string_feats(dom: str) -> dict:
    d = dom or ""
    feats = {}
    feats["len"] = len(d)
    feats["digits"] = sum(ch.isdigit() for ch in d)
    feats["hyphens"] = d.count("-")
    feats["dots"] = d.count(".")
    feats["digit_ratio"] = feats["digits"] / (feats["len"] + 1e-6)
    feats["hyphen_ratio"] = feats["hyphens"] / (feats["len"] + 1e-6)
    feats["entropy"] = shannon_entropy(d)
    feats["xn_punycode"] = int("xn--" in d)
    parts = d.split(".")
    feats["labels"] = len([p for p in parts if p])
    feats["tld_len"] = len(parts[-1]) if parts else 0
    return feats


DNS_NUM_COLS = ["num_unique_ips", "has_ipv6", "num_countries", "num_asns"]
WHOIS_NUM_COLS = [
    "age_days",
    "days_to_expiry",
    "created_isnull",
    "expires_isnull",
    "has_error",
]


def cat_row(r):
    """
    Build the same hashed categorical dict as training:
    ASN / ISP / country / registrar / status.
    For CT we usually only have sample_asn / sample_isp / sample_country.
    """
    d = {}

    asn = r.get("sample_asn")
    if asn is not None and not (isinstance(asn, float) and math.isnan(asn)):
        asn_str = str(asn).strip()
        if asn_str and asn_str.lower() != "nan":
            d["asn=" + asn_str] = 1

    isp = r.get("sample_isp")
    if isp is not None and not (isinstance(isp, float) and math.isnan(isp)):
        d["isp=" + str(isp)] = 1

    cc = r.get("sample_country")
    if cc is not None and not (isinstance(cc, float) and math.isnan(cc)):
        d["cc=" + str(cc)] = 1

    reg = r.get("registrar")
    if reg is not None and not (isinstance(reg, float) and math.isnan(reg)):
        d["reg=" + str(reg)] = 1

    st = r.get("status")
    if st is not None and not (isinstance(st, float) and math.isnan(st)):
        d["status=" + str(st)] = 1

    return d


# ---------- load enriched CT ----------

def load_enriched() -> pd.DataFrame:
    paths = sorted(
        glob.glob(os.path.join(ENRICHED_DIR, "**", "*.parquet"), recursive=True)
    )[-MAX_ENRICHED_FILES:]
    if not paths:
        print("[score] no enriched CT parquet found under", ENRICHED_DIR)
        return pd.DataFrame()

    dfs = []
    for p in paths:
        try:
            dfs.append(pd.read_parquet(p))
        except Exception as e:
            print("[score] skip file", p, "error:", e)

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    # Make sure registered_domain is present
    if "registered_domain" not in df.columns:
        if "domain_sample" in df.columns:
            df["registered_domain"] = df["domain_sample"].map(reg_domain)
        elif "domain" in df.columns:
            df["registered_domain"] = df["domain"].map(reg_domain)
        else:
            print("[score] no domain/registered_domain column present")
            return pd.DataFrame()

    df["registered_domain"] = (
        df["registered_domain"].astype(str).str.lower().str.strip()
    )
    df = df[df["registered_domain"].astype(bool)]

    # keep last record per registered_domain if duplicated
    df = (
        df.sort_values("event_ts") if "event_ts" in df.columns else df
    ).drop_duplicates("registered_domain", keep="last")

    print(
        f"[score] loaded {len(df)} enriched rows from {len(paths)} files "
        f"({df['registered_domain'].nunique()} unique regs)"
    )
    return df


# ---------- feature builder (must match week5) ----------

def build_features(df: pd.DataFrame):
    domains = df["registered_domain"].fillna("")

    # 1) char n-gram hashing
    char_vect = HashingVectorizer(
        analyzer="char",
        ngram_range=(3, 5),
        n_features=4096,
        lowercase=True,
        alternate_sign=False,
    )
    X_char = char_vect.transform(domains.tolist())

    # 2) basic numeric string features
    string_feats = [
        "len",
        "digits",
        "hyphens",
        "dots",
        "digit_ratio",
        "hyphen_ratio",
        "entropy",
        "xn_punycode",
        "labels",
        "tld_len",
    ]
    S = np.vstack(
        [
            [basic_string_feats(d).get(k, 0) for k in string_feats]
            for d in domains.tolist()
        ]
    )
    X_string = csr_matrix(S)

    # 3) DNS numeric
    for c in DNS_NUM_COLS:
        if c not in df.columns:
            df[c] = 0
    df[DNS_NUM_COLS] = df[DNS_NUM_COLS].fillna(0)
    X_dns_num = csr_matrix(df[DNS_NUM_COLS].to_numpy(dtype=float))

    # 4) hashed categoricals
    cats = [cat_row(r) for r in df.to_dict(orient="records")]
    hasher = FeatureHasher(
        n_features=256, input_type="dict", alternate_sign=False
    )
    X_cat = hasher.transform(cats)

    # 5) WHOIS numeric (we don't have WHOIS for CT, so zeros)
    for c in WHOIS_NUM_COLS:
        if c not in df.columns:
            df[c] = 0
    df[WHOIS_NUM_COLS] = df[WHOIS_NUM_COLS].fillna(0)
    X_whois_num = csr_matrix(df[WHOIS_NUM_COLS].to_numpy(dtype=float))

    # Final full matrix (shape must be 4371 cols)
    X_full = hstack([X_char, X_string, X_dns_num, X_cat, X_whois_num]).tocsr()
    print("[score] X_full shape:", X_full.shape)
    return X_full


# ---------- model loading ----------

def load_week5_model():
    """
    Prefer LightGBM full booster if available, otherwise fall back
    to LogisticRegression full.
    """
    lgb_path = os.path.join(MODEL_REG_DIR, "week5_lgbm_full.txt")
    logreg_path = os.path.join(MODEL_REG_DIR, "week5_logreg_full.joblib")

    # Try LightGBM booster first
    if lgb is not None and os.path.exists(lgb_path):
        print("[score] using LightGBM booster:", lgb_path)
        booster = lgb.Booster(model_file=lgb_path)

        def predict_proba(X):
            # Booster.predict returns probability for binary objective
            p = booster.predict(X)
            # ensure 1D array
            return np.asarray(p, dtype=float)

        return "week5_lgbm_full", predict_proba

    # Fallback: Logistic Regression
    if joblib is None or not os.path.exists(logreg_path):
        raise SystemExit(
            "[score] no usable model found "
            f"(missing {lgb_path} and/or {logreg_path})"
        )

    print("[score] using LogisticRegression:", logreg_path)
    clf = joblib.load(logreg_path)

    def predict_proba(X):
        # scikit predict_proba -> [:,1] for positive class
        return clf.predict_proba(X)[:, 1]

    return "week5_logreg_full", predict_proba


# ---------- main ----------

def main():
    df = load_enriched()
    if df.empty:
        print("[score] nothing to score")
        return

    X_full = build_features(df)
    model_name, predict_proba = load_week5_model()

    scores = predict_proba(X_full)
    if scores.shape[0] != len(df):
        raise RuntimeError(
            f"score length mismatch: {scores.shape[0]} vs {len(df)}"
        )

    out = df.copy()
    out["week5_score"] = scores

    # Optional: simple risk buckets
    out["risk_bucket"] = pd.cut(
        out["week5_score"],
        bins=[0.0, 0.2, 0.5, 0.8, 1.0],
        labels=["low", "medium", "high", "critical"],
        include_lowest=True,
    )

    # sort so head() is interesting
    out = out.sort_values("week5_score", ascending=False)

    ts = NOW_UTC.strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(SCORED_DIR, f"ct_scored_{ts}.parquet")
    out.to_parquet(out_path, index=False)

    print(
        f"[score] wrote {len(out)} scored rows → {out_path} "
        f"(model={model_name})"
    )

    # quick preview
    cols = [
        "registered_domain",
        "domain_sample" if "domain_sample" in out.columns else "registered_domain",
        "sample_country" if "sample_country" in out.columns else None,
        "num_unique_ips" if "num_unique_ips" in out.columns else None,
        "week5_score",
        "risk_bucket",
    ]
    cols = [c for c in cols if c is not None and c in out.columns]
    print("\n[score] top 20 risky domains:")
    print(out[cols].head(20).to_string(index=False))


if __name__ == "__main__":
    main()
