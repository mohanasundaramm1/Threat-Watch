# airflow/dags/dns_ip_geo_dag.py
from __future__ import annotations
import os, time, ipaddress, tempfile, logging
from datetime import datetime
from typing import List, Dict, Any, Tuple

import pandas as pd
import requests
import dns.resolver
import tldextract
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

log = logging.getLogger(__name__)

LABELS_UNION_BASE = "/opt/airflow/silver/labels_union"
LOOKUPS_BASE      = "/opt/airflow/lookups"

# Geo provider — env-var GEO_PROVIDER controls which backend is used:
#   ip_api      (default) — http://ip-api.com/batch, 45 req/min, HTTP only on free tier
#   ipinfo_lite — https://ipinfo.io, 50k/day free, set IPINFO_TOKEN too
#   maxmind     — local GeoLite2 .mmdb (zero API calls), set GEOLITE2_CITY_PATH + GEOLITE2_ASN_PATH
GEO_PROVIDER   = os.getenv("GEO_PROVIDER", "ip_api")
IPINFO_TOKEN   = os.getenv("IPINFO_TOKEN", "")
GEOLITE2_CITY_PATH = os.getenv("GEOLITE2_CITY_PATH", "/opt/airflow/geoip/GeoLite2-City.mmdb")
GEOLITE2_ASN_PATH  = os.getenv("GEOLITE2_ASN_PATH",  "/opt/airflow/geoip/GeoLite2-ASN.mmdb")

DNS_GEO_CACHE_PATH = f"{LOOKUPS_BASE}/dns_geo_cache.parquet"   # rolling cache
DNS_GEO_DAILY_DIR  = f"{LOOKUPS_BASE}/dns_geo"                 # partitioned snapshots


# ---------------- helpers ----------------

def _effective_ds_ts(ds: str, ts: str, **context):
    """
    Use ds/ts from dag_run.conf when triggered by the orchestrator; fall back to Airflow context.
    """
    dr = context.get("dag_run")
    if dr and getattr(dr, "conf", None):
        return dr.conf.get("ds", ds), dr.conf.get("ts", ts)
    return ds, ts

def _atomic_to_parquet(df: pd.DataFrame, final_path: str):
    """Atomic write to avoid partial files if the process dies."""
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=os.path.dirname(final_path), delete=False, suffix=".parquet") as tmp:
        tmp_path = tmp.name
    try:
        df.to_parquet(tmp_path, index=False)
        os.replace(tmp_path, final_path)  # atomic on POSIX
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass

def _load_labels_for_day(ds: str) -> pd.DataFrame:
    p = f"{LABELS_UNION_BASE}/ingest_date={ds}/labels_union.parquet"
    if not os.path.exists(p):
        raise AirflowSkipException(f"[dns_ip_geo] labels_union parquet not found for ds={ds}: {p}")
    df = pd.read_parquet(p).dropna(subset=["domain"])
    if df.empty:
        raise AirflowSkipException(f"[dns_ip_geo] labels_union empty for ds={ds}")
    return df

def _regdom(d: str) -> str:
    ext = tldextract.extract((d or "").strip())
    return (ext.registered_domain or "").lower()

def _to_punycode(d: str) -> str:
    try:
        return d.encode("idna").decode("ascii")
    except Exception:
        return d  # best effort; leave as-is

def _requests_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=4, connect=3, read=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update({"User-Agent": "threat-intel-lab/dns-geo/0.1"})
    return sess

def _resolve_rr(domain: str, rdtype: str, resolver: dns.resolver.Resolver) -> List[str]:
    ips: List[str] = []
    try:
        answers = resolver.resolve(domain, rdtype, lifetime=3)
        for rr in answers:
            addr = getattr(rr, "address", None)
            if addr:
                try:
                    ipaddress.ip_address(addr)   # validates v4/v6
                    ips.append(addr)
                except ValueError:
                    pass
    except Exception:
        pass
    return ips

def _resolve_ips(puny_domain: str, resolver: dns.resolver.Resolver) -> List[str]:
    # Resolve both A and AAAA
    return _resolve_rr(puny_domain, "A", resolver) + _resolve_rr(puny_domain, "AAAA", resolver)

def _ip_geo_batch(session: requests.Session, ips: List[str]) -> Dict[str, Dict[str, Any]]:
    """Route to the configured geo provider. Respects GEO_PROVIDER env-var."""
    if not ips:
        return {}
    if GEO_PROVIDER == "maxmind" and os.path.exists(GEOLITE2_CITY_PATH):
        return _ip_geo_maxmind(ips)
    if GEO_PROVIDER == "ipinfo_lite" and IPINFO_TOKEN:
        return _ip_geo_ipinfo(session, ips)
    # Default: ip-api free tier (HTTP only; HTTPS requires paid key)
    return _ip_geo_ipapi(session, ips)


def _ip_geo_maxmind(ips: List[str]) -> Dict[str, Dict[str, Any]]:
    """Local GeoLite2 City + ASN lookup \u2014 zero API calls, HTTPS safe, offline-capable.
    Requires GEOLITE2_CITY_PATH and optionally GEOLITE2_ASN_PATH env-vars pointing
    to downloaded .mmdb files. Run scripts/download_geolite2.sh to obtain them."""
    try:
        import geoip2.database
    except ImportError:
        log.warning("geoip2 not installed; falling back to ip-api. Run: pip install geoip2")
        from requests import Session
        return _ip_geo_ipapi(Session(), ips)

    out_map: Dict[str, Dict[str, Any]] = {}
    city_reader = geoip2.database.Reader(GEOLITE2_CITY_PATH)
    asn_reader  = geoip2.database.Reader(GEOLITE2_ASN_PATH) if os.path.exists(GEOLITE2_ASN_PATH) else None

    for ip in ips:
        try:
            city = city_reader.city(ip)
            asn_data = asn_reader.asn(ip) if asn_reader else None
            out_map[ip] = {
                "country":  city.country.name,
                "region":   city.subdivisions.most_specific.name if city.subdivisions else None,
                "city":     city.city.name,
                "lat":      float(city.location.latitude) if city.location.latitude else None,
                "lon":      float(city.location.longitude) if city.location.longitude else None,
                "asn":      f"AS{asn_data.autonomous_system_number}" if asn_data else None,
                "asn_name": asn_data.autonomous_system_organization if asn_data else None,
                "isp":      asn_data.autonomous_system_organization if asn_data else None,
                "org":      asn_data.autonomous_system_organization if asn_data else None,
                "reverse":  None,
                "proxy":    None,
                "hosting":  None,
            }
        except Exception:
            out_map[ip] = {}

    city_reader.close()
    if asn_reader:
        asn_reader.close()
    return out_map


def _ip_geo_ipapi(session: requests.Session, ips: List[str]) -> Dict[str, Dict[str, Any]]:
    """ip-api.com batch — free tier uses HTTP (not HTTPS on free plan)."""
    url = "http://ip-api.com/batch"  # free-tier constraint; use paid HTTPS if available
    out_map: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(ips), 100):
        chunk = ips[i:i+100]
        body = [{"query": ip, "fields": "status,country,regionName,city,lat,lon,isp,org,as,asname,reverse,proxy,hosting,query"}
                for ip in chunk]
        try:
            r = session.post(url, json=body, timeout=(3, 20))
            r.raise_for_status()
            payload = r.json()
            for rec in payload:
                ip = rec.get("query")
                if not ip:
                    continue
                if rec.get("status") != "success":
                    out_map[ip] = {}
                    continue
                out_map[ip] = {
                    "country": rec.get("country"),
                    "region": rec.get("regionName"),
                    "city": rec.get("city"),
                    "lat": rec.get("lat"),
                    "lon": rec.get("lon"),
                    "asn": (rec.get("as") or "").split(" ")[0] if rec.get("as") else None,
                    "asn_name": rec.get("asname"),
                    "isp": rec.get("isp"),
                    "org": rec.get("org"),
                    "reverse": rec.get("reverse"),
                    "proxy": rec.get("proxy"),
                    "hosting": rec.get("hosting"),
                }
        except Exception as e:
            log.warning("ip-api batch error on %s..%s: %s", chunk[:1], chunk[-1:], e)
            for ip in chunk:
                out_map[ip] = {}
        time.sleep(1.5)  # free-tier: 45 req/min, ~1 per 100-IP batch
    return out_map


def _ip_geo_ipinfo(session: requests.Session, ips: List[str]) -> Dict[str, Dict[str, Any]]:
    """ipinfo.io lite — supports HTTPS on free tier (50k req/day)."""
    out_map: Dict[str, Dict[str, Any]] = {}
    for ip in ips:
        try:
            r = session.get(f"https://ipinfo.io/{ip}/json?token={IPINFO_TOKEN}", timeout=(3, 10))
            r.raise_for_status()
            rec = r.json()
            country_code = rec.get("country", "")
            loc = rec.get("loc", ",").split(",")
            out_map[ip] = {
                "country": country_code,
                "region": rec.get("region"),
                "city": rec.get("city"),
                "lat": float(loc[0]) if len(loc) == 2 else None,
                "lon": float(loc[1]) if len(loc) == 2 else None,
                "asn": (rec.get("org") or "").split(" ")[0],
                "asn_name": (rec.get("org") or ""),
                "isp": rec.get("org"),
                "org": rec.get("org"),
                "reverse": rec.get("hostname"),
                "proxy": None,
                "hosting": None,
            }
        except Exception as e:
            log.warning("ipinfo error for %s: %s", ip, e)
            out_map[ip] = {}
        time.sleep(0.05)  # ~20 req/s (well under 50k/day)
    return out_map


# ---------------- main task ----------------

def dns_ip_geo_task(ds: str, ts: str, **context):
    ds, ts = _effective_ds_ts(ds, ts, **context)

    os.makedirs(LOOKUPS_BASE, exist_ok=True)
    os.makedirs(DNS_GEO_DAILY_DIR, exist_ok=True)
    outdir = f"{DNS_GEO_DAILY_DIR}/ingest_date={ds}"

    labels = _load_labels_for_day(ds)

    # normalize to registered domain + punycode
    labels["reg_domain"] = labels["domain"].map(_regdom)
    labels = labels[labels["reg_domain"] != ""].copy()
    if labels.empty:
        raise AirflowSkipException(f"[dns_ip_geo] no valid registered domains for ds={ds}")

    labels["puny_domain"] = labels["reg_domain"].map(_to_punycode)
    domains = sorted(labels["puny_domain"].dropna().unique().tolist())

    # load persistent cache (tolerate corruption)
    if os.path.exists(DNS_GEO_CACHE_PATH):
        try:
            cache = pd.read_parquet(DNS_GEO_CACHE_PATH)
        except Exception as e:
            log.warning("DNS/GEO cache unreadable (%s). Recreating empty cache.", e)
            cache = pd.DataFrame(columns=[
                "puny_domain","ip","family","country","region","city","lat","lon","asn","asn_name","isp","org","reverse","proxy","hosting"
            ])
    else:
        cache = pd.DataFrame(columns=[
            "puny_domain","ip","family","country","region","city","lat","lon","asn","asn_name","isp","org","reverse","proxy","hosting"
        ])

    cache["puny_domain"] = cache["puny_domain"].astype(str)
    cache["ip"] = cache["ip"].astype(str)
    cached_keys = set((cache["puny_domain"] + "|" + cache["ip"]).tolist())

    # DNS resolve (with fallback nameservers)
    resolver = dns.resolver.Resolver(configure=True)
    resolver.lifetime = 3.0
    resolver.timeout  = 3.0
    if not resolver.nameservers:
        resolver.nameservers = ["8.8.8.8", "1.1.1.1"]

    pairs: List[Tuple[str, str]] = []  # (puny_domain, ip)
    for d in domains:
        ips = _resolve_ips(d, resolver)
        for ip in ips:
            key = f"{d}|{ip}"
            if key not in cached_keys:
                pairs.append((d, ip))

    unique_ips = sorted({ip for _, ip in pairs})
    sess = _requests_session()
    geo_map = _ip_geo_batch(sess, unique_ips) if unique_ips else {}

    def _family(ip: str) -> int | None:
        try:
            return 6 if isinstance(ipaddress.ip_address(ip), ipaddress.IPv6Address) else 4
        except Exception:
            return None

    new_rows = []
    for d, ip in pairs:
        g = geo_map.get(ip, {}) or {}
        new_rows.append({
            "puny_domain": d,
            "ip": ip,
            "family": _family(ip),
            "country": g.get("country"),
            "region": g.get("region"),
            "city": g.get("city"),
            "lat": g.get("lat"),
            "lon": g.get("lon"),
            "asn": g.get("asn"),
            "asn_name": g.get("asn_name"),
            "isp": g.get("isp"),
            "org": g.get("org"),
            "reverse": g.get("reverse"),
            "proxy": g.get("proxy"),
            "hosting": g.get("hosting"),
        })

    new_df = pd.DataFrame(new_rows) if new_rows else pd.DataFrame(columns=cache.columns)

    # merge + dedupe cache on (puny_domain, ip)
    out_cache = pd.concat([cache, new_df], ignore_index=True)
    out_cache = out_cache.drop_duplicates(subset=["puny_domain","ip"], keep="last")

    # persist cache (atomic)
    _atomic_to_parquet(out_cache, DNS_GEO_CACHE_PATH)

    # daily subset = rows for today's domains (regardless of when first seen)
    daily = out_cache[out_cache["puny_domain"].isin(domains)].copy()
    _atomic_to_parquet(daily, f"{outdir}/dns_geo.parquet")

    log.info("[dns_ip_geo] ds=%s domains=%d new_pairs=%d daily_rows=%d cache_size=%d -> %s",
             ds, len(domains), len(pairs), len(daily), len(out_cache), outdir)

    # --- emit run manifest ---
    import json as _json
    manifest = {
        "dag_id": "dns_ip_geo_ingest",
        "ds": ds,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "total_domains": len(domains),
        "new_pairs_resolved": len(pairs),
        "daily_rows": len(daily),
        "cache_size": len(out_cache),
        "geo_provider": GEO_PROVIDER,
        "errors": 0,
    }
    manifest_path = f"{outdir}/run_manifest.json"
    with open(manifest_path, "w") as mf:
        _json.dump(manifest, mf, indent=2)
    log.info("[dns_ip_geo] manifest written to %s", manifest_path)


# ---------------- DAG ----------------

default_args = {"owner": "you", "retries": 3, "retry_delay": __import__("datetime").timedelta(minutes=5)}

with DAG(
    dag_id="dns_ip_geo_ingest",
    start_date=datetime(2025, 11, 1),
    schedule_interval=None,          # <-- trigger-only (orchestrator handles ds/ts)
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["enrich","dns","geo","lookup"],
) as dag:
    PythonOperator(
        task_id="dns_ip_geo_fetch",
        python_callable=dns_ip_geo_task,
    )
