# airflow/dags/whois_rdap_dag.py
from __future__ import annotations

import os
import time
import json
import tempfile
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List

import pandas as pd
import requests
import tldextract
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.exceptions import AirflowSkipException

log = logging.getLogger(__name__)

# --------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------

LABELS_UNION_BASE = "/opt/airflow/silver/labels_union"
LOOKUPS_BASE = "/opt/airflow/lookups"

WHOIS_CACHE_PATH = f"{LOOKUPS_BASE}/whois_cache.parquet"   # rolling cache
WHOIS_DAILY_DIR = f"{LOOKUPS_BASE}/whois"                  # partitioned snapshots

# TTL knobs (override via env if needed)
TTL_DAYS = int(os.getenv("WHOIS_TTL_DAYS", "30"))                  # refresh good rows after N days
ERROR_REFRESH_DAYS = int(os.getenv("WHOIS_ERROR_REFRESH_DAYS", "7"))  # retry errored rows after N days

# --------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------


def _effective_ds_ts(ds: str, ts: str, **context):
    """
    If orchestrator passed ds/ts via dag_run.conf, prefer those.
    Otherwise keep the scheduler's ds/ts.
    """
    dr = context.get("dag_run")
    if dr and getattr(dr, "conf", None):
        ds2 = dr.conf.get("ds") or ds
        ts2 = dr.conf.get("ts") or ts
        if ds2 != ds or ts2 != ts:
            log.info(
                "Overriding ds/ts from dag_run.conf: %s,%s -> %s,%s",
                ds, ts, ds2, ts2
            )
        return ds2, ts2
    return ds, ts


def _resolve_ds_ts_flexible(ds: str | None, ts: str | None, **maybe_ctx):
    """
    Works whether ds/ts/context are passed explicitly or not.
    Fallbacks:
      - Airflow runtime context (get_current_context)
      - Now in UTC if no context is available (e.g. ad-hoc local test)
    """
    try:
        ctx = maybe_ctx if ("dag_run" in maybe_ctx or "ds" in maybe_ctx) else get_current_context()
        ds0 = ds or ctx["ds"]
        ts0 = ts or ctx["ts"]
        return _effective_ds_ts(ds0, ts0, **ctx)
    except Exception:
        now = pd.Timestamp.now(tz="UTC")
        return now.date().isoformat(), now.strftime("%Y-%m-%d %H:%M:%S")


def _atomic_to_parquet(df: pd.DataFrame, final_path: str):
    """
    Atomic write: write to a temp file then move into place.
    Prevents half-written files if the process dies.
    """
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        suffix=".parquet",
        dir=os.path.dirname(final_path) or ".",
    )
    os.close(fd)
    try:
        df.to_parquet(tmp, index=False)
        os.replace(tmp, final_path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass


def _load_labels_for_day(ds: str) -> pd.DataFrame:
    """
    Load labels_union for a given ds, or skip if missing/empty.
    """
    p = f"{LABELS_UNION_BASE}/ingest_date={ds}/labels_union.parquet"
    if not os.path.exists(p):
        raise AirflowSkipException(
            f"[whois_rdap] labels_union parquet not found for ds={ds}: {p}"
        )
    df = pd.read_parquet(p).dropna(subset=["domain"])
    if df.empty:
        raise AirflowSkipException(
            f"[whois_rdap] labels_union empty for ds={ds}, nothing to enrich."
        )
    return df


def _effective_domain(d: str) -> str:
    """
    Normalize to registered domain (example.com from foo.bar.example.com).
    Handles both registered_domain and top_domain_under_public_suffix so
    we’re safe across tldextract versions.
    """
    ext = tldextract.extract(d or "")
    reg = (
        getattr(ext, "registered_domain", None)
        or getattr(ext, "top_domain_under_public_suffix", None)
        or ""
    )
    return reg.lower().strip()


def _requests_session() -> requests.Session:
    """
    HTTP session with retry/backoff tuned for RDAP.
    """
    sess = requests.Session()
    retry = Retry(
        total=4,
        connect=3,
        read=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods={"GET"},
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update(
        {
            "User-Agent": "threat-intel-lab/rdap/0.1",
            "Accept": "application/rdap+json, application/json;q=0.8, */*;q=0.5",
        }
    )
    return sess


def _parse_registrar(data: dict) -> str | None:
    """
    RDAP is messy about where registrar appears; best-effort extraction.
    """
    if "registrar" in data and isinstance(data["registrar"], dict):
        name = data["registrar"].get("name")
        if name:
            return name

    for ent in (data.get("entities") or []):
        if "registrar" in (ent.get("roles") or []):
            vcard = ent.get("vcardArray") or []
            if isinstance(vcard, list) and len(vcard) == 2:
                for item in vcard[1]:
                    if item and item[0] == "fn":
                        return item[3]

    return None


def _rdap_domain(domain: str, session: requests.Session) -> Dict[str, Any]:
    """
    Fetch RDAP for a domain and normalize into a single row.
    """
    url = f"https://rdap.org/domain/{domain}"
    try:
        r = session.get(url, timeout=(3, 30))
        if r.status_code == 404:
            return {
                "domain": domain,
                "registrar": None,
                "status": "not_found",
                "created": pd.NaT,
                "expires": pd.NaT,
                "raw": None,
                "error": None,
                "fetched_at": pd.Timestamp.now(tz="UTC"),
            }

        r.raise_for_status()
        data = r.json()

        registrar = _parse_registrar(data)
        statuses = ",".join(data.get("status") or [])

        created = None
        expires = None
        for ev in (data.get("events") or []):
            if ev.get("eventAction") == "registration":
                created = ev.get("eventDate")
            if ev.get("eventAction") in ("expiration", "expire"):
                expires = ev.get("eventDate")

        return {
            "domain": domain,
            "registrar": registrar,
            "status": statuses or None,
            "created": pd.to_datetime(created, utc=True, errors="coerce"),
            "expires": pd.to_datetime(expires, utc=True, errors="coerce"),
            "raw": json.dumps(data)[:200_000],
            "error": None,
            "fetched_at": pd.Timestamp.now(tz="UTC"),
        }

    except Exception as e:
        return {
            "domain": domain,
            "registrar": None,
            "status": None,
            "created": pd.NaT,
            "expires": pd.NaT,
            "raw": None,
            "error": str(e),
            "fetched_at": pd.Timestamp.now(tz="UTC"),
        }


# --------------------------------------------------------------------
# Main task
# --------------------------------------------------------------------


def whois_rdap_task(ds: str | None = None, ts: str | None = None, **context):
    # Resolve ds/ts, preferring dag_run.conf when triggered by orchestrator
    ds, ts = _resolve_ds_ts_flexible(ds, ts, **context)

    os.makedirs(LOOKUPS_BASE, exist_ok=True)
    os.makedirs(WHOIS_DAILY_DIR, exist_ok=True)

    labels = _load_labels_for_day(ds)
    labels["registered_domain"] = labels["domain"].map(_effective_domain)

    domains = sorted(
        d
        for d in labels["registered_domain"]
        .dropna()
        .astype(str)
        .str.lower()
        .unique()
        .tolist()
        if d
    )
    if not domains:
        raise AirflowSkipException(f"[whois_rdap] no valid domains for ds={ds}")

    # Load persistent cache, but be tolerant to corruption
    if os.path.exists(WHOIS_CACHE_PATH):
        try:
            cache = pd.read_parquet(WHOIS_CACHE_PATH)
        except Exception as e:
            log.warning(
                "WHOIS cache unreadable (%s). Recreating empty cache.", e
            )
            cache = pd.DataFrame(
                columns=[
                    "domain",
                    "registrar",
                    "status",
                    "created",
                    "expires",
                    "raw",
                    "error",
                    "fetched_at",
                ]
            )
    else:
        cache = pd.DataFrame(
            columns=[
                "domain",
                "registrar",
                "status",
                "created",
                "expires",
                "raw",
                "error",
                "fetched_at",
            ]
        )

    # Ensure expected columns exist
    for c in ["domain", "registrar", "status", "raw", "error"]:
        if c not in cache.columns:
            cache[c] = None
    for c in ["created", "expires", "fetched_at"]:
        if c not in cache.columns:
            cache[c] = pd.NaT

    # Normalize types
    cache["domain"] = cache["domain"].astype(str).str.lower()
    for c in ["created", "expires", "fetched_at"]:
        cache[c] = pd.to_datetime(cache[c], utc=True, errors="coerce")

    # TTL logic (all tz-aware)
    now = pd.Timestamp.now(tz="UTC")
    age = now - cache["fetched_at"]
    is_fresh = age < pd.Timedelta(days=TTL_DAYS)
    is_error = cache["error"].notna() & (cache["error"].astype(str) != "")
    error_stale = age >= pd.Timedelta(days=ERROR_REFRESH_DAYS)

    cached_domains = set(cache["domain"])
    stale_domains = set(cache.loc[~is_fresh, "domain"])
    error_domains_to_retry = set(cache.loc[is_error & error_stale, "domain"])

    to_fetch = [
        d
        for d in domains
        if (d not in cached_domains)
        or (d in stale_domains)
        or (d in error_domains_to_retry)
    ]

    log.info(
        "[whois_rdap] ds=%s total=%d to_fetch=%d (stale=%d, error_retry=%d) cached=%d",
        ds,
        len(domains),
        len(to_fetch),
        len(stale_domains & set(domains)),
        len(error_domains_to_retry & set(domains)),
        len(cached_domains),
    )

    # Fetch new/refresh domains
    session = _requests_session()
    new_rows: List[dict] = []

    for i, d in enumerate(to_fetch, 1):
        new_rows.append(_rdap_domain(d, session))

        if i % 50 == 0:
            log.info("[whois_rdap] progress: %d/%d domains processed", i, len(to_fetch))

        # light throttle to avoid hammering RDAP service
        if i % 10 == 0:
            time.sleep(1)

    if new_rows:
        new_df = pd.DataFrame(new_rows)
    else:
        new_df = pd.DataFrame(columns=cache.columns)

    # Ensure columns in new_df
    for c in ["domain", "registrar", "status", "created", "expires", "raw", "error", "fetched_at"]:
        if c not in new_df.columns:
            new_df[c] = None

    new_df["domain"] = new_df["domain"].astype(str).str.lower()
    for c in ["created", "expires", "fetched_at"]:
        new_df[c] = pd.to_datetime(new_df[c], utc=True, errors="coerce")

    # Merge cache + new rows; keep latest by fetched_at
    out_cache = pd.concat([cache, new_df], ignore_index=True)
    out_cache = (
        out_cache.sort_values(["domain", "fetched_at"], na_position="first")
        .drop_duplicates(subset=["domain"], keep="last")
    )

    # Persist global cache
    _atomic_to_parquet(out_cache, WHOIS_CACHE_PATH)

    # Daily subset for this ds
    daily = out_cache[out_cache["domain"].isin(domains)].copy()
    outdir = f"{WHOIS_DAILY_DIR}/ingest_date={ds}"
    outpath = f"{outdir}/whois.parquet"
    _atomic_to_parquet(daily, outpath)

    log.info(
        "[whois_rdap] ds=%s fetched=%d daily_rows=%d cache_size=%d -> %s",
        ds,
        len(new_rows),
        len(daily),
        len(out_cache),
        outpath,
    )


# --------------------------------------------------------------------
# DAG definition
# --------------------------------------------------------------------

default_args = {
    "owner": "you",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="whois_rdap_ingest",
    start_date=datetime(2025, 11, 1),
    schedule_interval=None,  # orchestrator triggers
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["enrich", "whois", "lookup"],
) as dag:
    PythonOperator(
        task_id="whois_rdap_fetch",
        python_callable=whois_rdap_task,
        execution_timeout=timedelta(hours=4),
    )
