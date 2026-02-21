# ct/enrich/enrich_ct.py

import os
import time
import glob
import json
import socket
import ipaddress
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import tldextract

# ---------- path setup ----------

THIS_DIR  = os.path.dirname(__file__)                      # .../ct/enrich
CT_DIR    = os.path.abspath(os.path.join(THIS_DIR, ".."))  # .../ct
REPO_ROOT = os.path.abspath(os.path.join(CT_DIR, ".."))    # .../threat-intel

DATA_DIR = os.path.join(CT_DIR, "data")

RAW_DIR_DEFAULT      = os.path.join(DATA_DIR, "raw")
ENRICHED_DIR_DEFAULT = os.path.join(DATA_DIR, "enriched")
CACHE_DEFAULT        = os.path.join(DATA_DIR, "ip_api_cache.json")

os.makedirs(ENRICHED_DIR_DEFAULT, exist_ok=True)

BRONZE     = os.getenv("CT_RAW_DIR", RAW_DIR_DEFAULT)
SILVER     = os.getenv("CT_ENRICHED_DIR", ENRICHED_DIR_DEFAULT)
CACHE_PATH = os.getenv("IP_API_CACHE", CACHE_DEFAULT)

MAX_BRONZE_FILES = int(os.getenv("MAX_BRONZE_FILES", "50"))
CAP_ROWS         = int(os.getenv("CAP_ROWS", "20000"))
MAX_DOMAINS      = int(os.getenv("MAX_DOMAINS", "1000"))
RESOLVE_THREADS  = int(os.getenv("RESOLVE_THREADS", "32"))


# ---------- helpers ----------

def reg_domain(d: str) -> str:
    if not isinstance(d, str) or not d:
        return ""
    ext = tldextract.extract(d)
    reg = getattr(ext, "registered_domain", None) or getattr(
        ext, "top_domain_under_public_suffix", None
    ) or ""
    return reg.lower().strip()


def read_bronze() -> pd.DataFrame:
    paths = sorted(glob.glob(f"{BRONZE}/**/*.parquet", recursive=True))[-MAX_BRONZE_FILES:]
    if not paths:
        print("no bronze data found in", BRONZE)
        return pd.DataFrame()

    dfs = []
    for p in paths:
        try:
            dfp = pd.read_parquet(p)
            keep = [c for c in ["domain", "event_ts", "source", "ingest_ts"] if c in dfp.columns]
            if keep:
                dfp = dfp[keep]
            dfs.append(dfp)
        except Exception as e:
            print("skip file", p, e)

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    if "ingest_ts" in df.columns:
        df = df.sort_values("ingest_ts").tail(CAP_ROWS)

    df["domain"] = (
        df["domain"]
        .astype(str)
        .str.lower()
        .str.strip()
        .str.rstrip(".")
        .str.replace("*.", "", regex=False)
    )
    df["registered_domain"] = df["domain"].map(reg_domain)
    df = df[df["registered_domain"].astype(bool)]

    # one row per registered domain for enrichment
    return df.drop_duplicates(subset=["registered_domain"])


def resolve_domain(d: str, timeout: float = 2.0):
    try:
        socket.setdefaulttimeout(timeout)
        infos = socket.getaddrinfo(d, None)
        return list({ai[4][0] for ai in infos if ai and ai[4]})
    except Exception:
        return []


def enrich_ip(ip: str, session: requests.Session):
    try:
        r = session.get(
            f"http://ip-api.com/json/{ip}?fields=status,country,as,asname,org,query",
            timeout=2.5,
        )
        j = r.json()
        if j.get("status") == "success":
            return {
                "country": j.get("country"),
                "asn": j.get("as"),
                "asname": j.get("asname"),
                "org": j.get("org"),
            }
    except Exception:
        pass
    return {"country": None, "asn": None, "asname": None, "org": None}


def load_cache():
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_cache(cache):
    tmp = CACHE_PATH + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(cache, f)
        os.replace(tmp, CACHE_PATH)
    except Exception as e:
        print("cache save failed:", e)


def is_ipv6(ip: str) -> int:
    try:
        return int(ipaddress.ip_address(ip).version == 6)
    except Exception:
        return 0


# ---------- main ----------

def main():
    df = read_bronze()
    if df.empty:
        print("no bronze data yet")
        return

    print(
        f"bronze rows: {len(df)} | unique registered domains: "
        f"{df['registered_domain'].nunique()}"
    )

    regs = df["registered_domain"].dropna().unique().tolist()[:MAX_DOMAINS]

    # Resolve in threads
    rows = []

    def _task(reg):
        ips = resolve_domain(reg)
        return reg, ips

    with ThreadPoolExecutor(max_workers=RESOLVE_THREADS) as ex:
        futs = {ex.submit(_task, reg): reg for reg in regs}
        for fut in as_completed(futs):
            reg, ips = fut.result()
            if not ips:
                rows.append({"registered_domain": reg, "ip": None})
            else:
                for ip in ips:
                    rows.append({"registered_domain": reg, "ip": ip})

    res_df = pd.DataFrame(rows)
    if res_df.empty:
        print("no IPs resolved")
        return

    # IP enrichment with cache
    cache = load_cache()
    session = requests.Session()
    enr_rows = []
    uniq_ips = [ip for ip in res_df["ip"].dropna().unique()]

    for ip in uniq_ips:
        if ip not in cache:
            cache[ip] = enrich_ip(ip, session)
            time.sleep(0.15)  # be polite to the free API
        enr_rows.append({"ip": ip, **cache[ip]})

    if uniq_ips:
        save_cache(cache)

    enr_df = (
        pd.DataFrame(enr_rows)
        if enr_rows
        else pd.DataFrame(columns=["ip", "country", "asn", "asname", "org"])
    )

    out = res_df.merge(enr_df, on="ip", how="left")

    agg = (
        out.groupby("registered_domain")
        .agg(
            num_unique_ips=("ip", "nunique"),
            has_ipv6=("ip", lambda s: int(any(is_ipv6(v) for v in s.dropna()))),
            num_countries=("country", "nunique"),
            num_asns=("asn", "nunique"),
            sample_asn=("asn", "first"),
            sample_isp=("asname", "first"),
            sample_country=("country", "first"),
        )
        .reset_index()
    )

    # attach one original sample domain / meta
    base = (
        df[["registered_domain", "domain", "event_ts", "source"]]
        .drop_duplicates("registered_domain")
    )
    agg = agg.merge(base, on="registered_domain", how="left").rename(
        columns={"domain": "domain_sample"}
    )

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(SILVER, f"ct_enriched_{ts}.parquet")
    agg.to_parquet(out_path, index=False)
    print(f"wrote enriched domain features: {len(agg)} rows → {out_path}")

    # Quick summary
    print(agg.head(15))
    coverage_ips = (
        (agg["num_unique_ips"] > 0).mean() * 100.0
        if "num_unique_ips" in agg
        else 0.0
    )
    print(f"coverage: {coverage_ips:.1f}% domains have at least one resolved IP")


if __name__ == "__main__":
    main()
