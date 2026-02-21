# ml/offline/analyze_week5_risk_clusters.py

import os, glob, math
import numpy as np
import pandas as pd
import tldextract
from datetime import datetime, timezone
from collections import Counter

NOW_UTC = datetime.now(timezone.utc)

# ---------------- paths ----------------

THIS_DIR = os.path.dirname(__file__)                    # .../ml/offline
ML_DIR = os.path.abspath(os.path.join(THIS_DIR, ".."))  # .../ml
REPO_ROOT = os.path.abspath(os.path.join(ML_DIR, "..")) # .../threat-intel

SILVER_LABELS_DIR = os.path.join(REPO_ROOT, "silver", "labels_union")
DNS_GEO_DIR       = os.path.join(REPO_ROOT, "lookups", "dns_geo")
WHOIS_DIR         = os.path.join(REPO_ROOT, "lookups", "whois")

# ---------------- config ----------------

PHISH_DAYS  = [f"2025-11-{d:02d}" for d in range(17, 27)]
BENIGN_DAYS = ["2025-10-28", "2025-10-29", "2025-10-30", "2025-10-31"]

# ---------------- helpers ----------------

def reg_domain(domain: str) -> str:
    if not isinstance(domain, str) or not domain:
        return ""
    ext = tldextract.extract(domain)
    reg = getattr(ext, "registered_domain", None) or getattr(
        ext, "top_domain_under_public_suffix", None
    ) or ""
    return reg.lower().strip()


def df_from_parquets(patterns):
    files = []
    for p in patterns:
        files.extend(glob.glob(p))
    dfs = []
    for f in sorted(files):
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as e:
            print(f"[warn] failed {f}: {e}")
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def load_dns_for_days(days):
    paths = [
        os.path.join(DNS_GEO_DIR, f"ingest_date={d}", "dns_geo.parquet")
        for d in days
    ]
    print("[info] loading DNS-Geo from:")
    for p in paths:
        print("  -", p)
    dns = df_from_parquets(paths)
    if dns.empty:
        print("[warn] no DNS-Geo data loaded.")
        return dns
    dns = dns.rename(columns={"puny_domain": "registered_domain"})
    dns["registered_domain"] = (
        dns["registered_domain"].astype(str).str.lower().str.strip()
    )
    return dns


def load_whois_for_days(days):
    paths = [
        os.path.join(WHOIS_DIR, f"ingest_date={d}", "whois.parquet")
        for d in days
    ]
    print("[info] loading WHOIS from:")
    for p in paths:
        print("  -", p)
    w = df_from_parquets(paths)
    if w.empty:
        print("[warn] no WHOIS data loaded.")
        return w
    w = w.copy()
    w["registered_domain"] = w["domain"].astype(str).str.lower().str.strip()
    return w


# ---------------- load labels & build Xdf ----------------

label_paths = [
    os.path.join(SILVER_LABELS_DIR, f"ingest_date={d}", "labels_union.parquet")
    for d in PHISH_DAYS + BENIGN_DAYS
]
print("[info] loading labels from:")
for p in label_paths:
    print("  -", p)

labels = df_from_parquets(label_paths)
if labels.empty:
    raise SystemExit("No labels found.")

labels["registered_domain"] = labels["domain"].map(reg_domain)
labels = labels[labels["registered_domain"].astype(bool)].copy()
labels["label"] = np.where(labels["source"] == "benign_seed", 0, 1)
labels = labels[labels["label"].isin([0, 1])].copy()
labels["ingest_date"] = labels["ingest_date"].astype(str)

print(
    "[info] labels after filtering:",
    len(labels),
    "positives=",
    int((labels["label"] == 1).sum()),
    "negatives=",
    int((labels["label"] == 0).sum()),
)

last_seen = (
    labels.groupby("registered_domain")["ingest_date"]
    .max()
    .reset_index()
    .rename(columns={"ingest_date": "last_ingest_date"})
)

dns = load_dns_for_days(PHISH_DAYS + BENIGN_DAYS)
if not dns.empty:
    dns_agg = (
        dns.groupby("registered_domain")
        .agg(
            num_unique_ips=("ip", "nunique"),
            num_countries=("country", "nunique"),
            sample_country=("country", "first"),
            sample_asn=("asn", "first"),
        )
        .reset_index()
    )
else:
    dns_agg = pd.DataFrame(columns=["registered_domain"])

whois = load_whois_for_days(PHISH_DAYS + BENIGN_DAYS)
if not whois.empty:
    wcols = ["registered_domain", "registrar", "status", "created", "expires", "error"]
    whois = whois[wcols].copy()
    whois["created"] = pd.to_datetime(
        whois["created"], utc=True, errors="coerce"
    )
    whois["expires"] = pd.to_datetime(
        whois["expires"], utc=True, errors="coerce"
    )
    whois["age_days"] = (
        NOW_UTC - whois["created"]
    ).dt.total_seconds() / 86400.0
    whois["days_to_expiry"] = (
        whois["expires"] - NOW_UTC
    ).dt.total_seconds() / 86400.0
else:
    whois = pd.DataFrame(columns=["registered_domain"])

Xdf = (
    labels[["registered_domain", "label"]]
    .drop_duplicates("registered_domain")
    .merge(last_seen, on="registered_domain", how="left")
    .merge(dns_agg, on="registered_domain", how="left")
    .merge(whois, on="registered_domain", how="left")
)

print("[info] Xdf rows:", len(Xdf))

# ---------------- feature buckets ----------------

# simple TLD
def tld_of(reg):
    if not isinstance(reg, str) or "." not in reg:
        return ""
    return reg.rsplit(".", 1)[-1]

Xdf["tld"] = Xdf["registered_domain"].map(tld_of)

# age buckets
def age_bucket(a):
    if pd.isna(a):
        return "age:unknown"
    if a < 7:
        return "age:<7d"
    if a < 30:
        return "age:7-30d"
    if a < 180:
        return "age:30-180d"
    if a < 365:
        return "age:180-365d"
    return "age:>365d"

Xdf["age_bucket"] = Xdf["age_days"].map(age_bucket)

# IP-count buckets
def ip_bucket(n):
    if pd.isna(n):
        return "ips:none"
    n = float(n)
    if n == 0:
        return "ips:0"
    if n == 1:
        return "ips:1"
    if n <= 3:
        return "ips:2-3"
    if n <= 10:
        return "ips:4-10"
    return "ips:>10"

Xdf["ip_bucket"] = Xdf["num_unique_ips"].map(ip_bucket)

# length / digit / entropy buckets
Xdf["dom_len"] = Xdf["registered_domain"].map(lambda d: len(d or ""))
Xdf["digit_ratio"] = Xdf["registered_domain"].map(
    lambda d: sum(ch.isdigit() for ch in (d or "")) / (len(d or "") + 1e-6)
)

def entropy(s):
    if not s:
        return 0.0
    c = Counter(s)
    n = len(s)
    return -sum((v / n) * math.log2(v / n) for v in c.values())

Xdf["entropy"] = Xdf["registered_domain"].map(entropy)

Xdf["len_bucket"] = pd.cut(
    Xdf["dom_len"],
    bins=[0, 10, 20, 30, 1000],
    labels=["len:<=10", "len:11-20", "len:21-30", "len:>30"],
    include_lowest=True,
)
Xdf["digit_bucket"] = pd.cut(
    Xdf["digit_ratio"],
    bins=[0.0, 0.1, 0.3, 1.0],
    labels=["digits:<10%", "digits:10-30%", "digits:>30%"],
    include_lowest=True,
)
Xdf["entropy_bucket"] = pd.cut(
    Xdf["entropy"],
    bins=[0.0, 2.5, 3.5, 10.0],
    labels=["entropy:low", "entropy:med", "entropy:high"],
    include_lowest=True,
)

# ---------------- group stats ----------------

def print_group_stats(name, key_col, min_count=20, top_n=20):
    g = (
        Xdf.groupby(key_col)["label"]
        .agg(
            count="size",
            phish="sum",
        )
        .reset_index()
    )
    g["benign"] = g["count"] - g["phish"]
    g["phish_rate"] = g["phish"] / g["count"]
    g = g[g["count"] >= min_count]
    g = g.sort_values(["phish_rate", "count"], ascending=[False, False])
    print(f"\n=== {name} (min_count={min_count}) ===")
    print(g.head(top_n).to_string(index=False))


# TLD risk
print_group_stats("TLD risk", "tld", min_count=30, top_n=25)

# Registrar risk
print_group_stats("Registrar risk", "registrar", min_count=30, top_n=25)

# Age buckets
print_group_stats("Age bucket risk", "age_bucket", min_count=30, top_n=20)

# IP buckets
print_group_stats("IP-count bucket risk", "ip_bucket", min_count=30, top_n=20)

# Length / digit / entropy buckets
print_group_stats("Domain length bucket risk", "len_bucket", min_count=30, top_n=20)
print_group_stats("Digit-ratio bucket risk", "digit_bucket", min_count=30, top_n=20)
print_group_stats("Entropy bucket risk", "entropy_bucket", min_count=30, top_n=20)

# Country-level risk (from DNS)
print_group_stats("Sample country risk (DNS)", "sample_country", min_count=30, top_n=25)
