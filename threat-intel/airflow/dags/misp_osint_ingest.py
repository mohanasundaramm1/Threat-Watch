# airflow/dags/misp_osint_ingest.py
from __future__ import annotations
import os, re, io, json, time, tempfile, logging
from datetime import datetime, timedelta
from typing import List, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.exceptions import AirflowSkipException

log = logging.getLogger(__name__)

# Where to write
SILVER_BASE = "/opt/airflow/silver/misp_osint"

# CIRCL OSINT feed root (Apache directory index of many *.json files)
DEFAULT_FEED_BASE = "https://www.circl.lu/doc/misp/feed-osint/"

# Regex to capture (href, last_modified) from the Apache directory table row.
# Example:
#   <td><a href="UUID.json">...</a></td><td align="right">2025-11-04 13:58</td>
ROW_RE = re.compile(
    r'href="([^"]+\.json)".{0,400}?</td><td[^>]*>\s*([0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2}(?::[0-9]{2})?)',
    re.IGNORECASE | re.DOTALL
)

# ---------------- helpers ----------------

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "threat-intel-lab/misp-osint/0.1", "Accept": "application/json"})
    retry = Retry(
        total=4, connect=3, read=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods={"GET"},
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def _atomic_to_parquet(df: pd.DataFrame, final_path: str):
    """Write atomically to avoid partial files on failure."""
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(suffix=".parquet", dir=os.path.dirname(final_path) or ".")
    os.close(fd)
    try:
        df.to_parquet(tmp, index=False)
        os.replace(tmp, final_path)
    finally:
        if os.path.exists(tmp):
            try: os.remove(tmp)
            except Exception: pass

def _effective_ds_ts(ds: str, ts: str, **context):
    """Honor dag_run.conf overrides from the orchestrator."""
    dr = context.get("dag_run")
    if dr and getattr(dr, "conf", None):
        ds2 = dr.conf.get("ds") or ds
        ts2 = dr.conf.get("ts") or ts
        if ds2 != ds or ts2 != ts:
            log.info("Overriding ds/ts from dag_run.conf: %s,%s -> %s,%s", ds, ts, ds2, ts2)
        return ds2, ts2
    return ds, ts

def _resolve_ds_ts_flexible(ds: str | None, ts: str | None, **maybe_ctx):
    """
    Works whether ds/ts/context were provided explicitly or not.
    Falls back to Airflow runtime context; last-resort fallback = 'now' (UTC).
    """
    try:
        ctx = maybe_ctx if ("dag_run" in maybe_ctx or "ds" in maybe_ctx) else get_current_context()
        ds0 = ds or ctx["ds"]
        ts0 = ts or ctx["ts"]
        return _effective_ds_ts(ds0, ts0, **ctx)
    except Exception:
        now = pd.Timestamp.utcnow()
        return (now.date().isoformat(), now.strftime("%Y-%m-%d %H:%M:%S"))

def _parse_index(base_url: str, session: requests.Session) -> pd.DataFrame:
    """Return a dataframe with ['url','last_modified'] from the index page."""
    r = session.get(base_url, timeout=(5, 45))
    r.raise_for_status()
    rows: List[Tuple[str, str]] = ROW_RE.findall(r.text)
    data = []
    for rel, lm in rows:
        absu = urljoin(base_url, rel)
        # Parse naive timestamp and treat as UTC (server usually lists UTC; if not, the day window has a buffer)
        dt = pd.to_datetime(lm, utc=True, errors="coerce")
        if pd.isna(dt):
            continue
        data.append((absu, dt))
    df = pd.DataFrame(data, columns=["url", "last_modified"]).drop_duplicates(subset=["url"])
    return df.sort_values("last_modified")

def _extract_attributes(event_json: dict) -> list[dict]:
    """
    Extract flat rows from a MISP event JSON.
    Columns: indicator, type, category, orgc, event_id, first_seen, last_seen, tags, galaxies, source
    """
    rows = []
    ev = event_json.get("Event") or event_json
    event_id = ev.get("id") or ev.get("uuid") or ""
    orgc = ""
    try:
        orgc = (ev.get("Orgc") or ev.get("orgc") or {}).get("name") or ""
    except Exception:
        pass

    tags = []
    for t in (ev.get("Tag") or ev.get("tags") or []):
        if isinstance(t, dict):
            name = t.get("name") or t.get("Name") or ""
            if name:
                tags.append(name)
        elif isinstance(t, str):
            tags.append(t)

    galaxies = []
    for g in (ev.get("Galaxy") or ev.get("galaxies") or []):
        if isinstance(g, dict) and g.get("name"):
            galaxies.append(g["name"])

    attributes = ev.get("Attribute") or ev.get("attributes") or []
    for a in attributes:
        indicator = a.get("value") or a.get("Value") or ""
        atype    = a.get("type")  or a.get("Type")  or ""
        cat      = a.get("category") or a.get("Category") or ""
        first    = a.get("first_seen") or ev.get("date") or None
        last     = a.get("last_seen") or None

        rows.append({
            "indicator": indicator,
            "type": atype,
            "category": cat,
            "orgc": orgc,
            "event_id": event_id,
            "first_seen": pd.to_datetime(first, utc=True, errors="coerce"),
            "last_seen":  pd.to_datetime(last,  utc=True, errors="coerce"),
            "tags": tags[:],
            "galaxies": galaxies[:],
            "source": "misp_osint",
        })
    return rows

# ---------------- main task ----------------

def ingest_misp_osint(ds: str | None = None, ts: str | None = None, **context):
    """
    Strategy:
      1) Parse the index into (url,last_modified).
      2) Keep files whose last_modified falls in the ds UTC window (with ±3h buffer).
      3) Fetch those JSONs (capped), extract attributes → flat rows.
      4) Write atomically to silver/misp_osint/ingest_date={ds}/misp_osint.parquet
         with a MIN_ROWS guard (preserve an existing same-day partition if present).
    """
    ds, ts = _resolve_ds_ts_flexible(ds, ts, **context)

    base_url   = (os.getenv("MISP_OSINT_BASE") or DEFAULT_FEED_BASE).strip()
    max_files  = int(os.getenv("MISP_OSINT_MAX", "100"))           # safety cap per run
    rate_sec   = float(os.getenv("MISP_OSINT_RATE_SEC", "0.3"))    # small delay between fetches
    timeout    = (5, int(os.getenv("MISP_OSINT_TIMEOUT", "60")))
    MIN_ROWS   = int(os.getenv("MISP_OSINT_MIN_ROWS", "50"))

    s = _session()

    # 1) index
    idx = _parse_index(base_url, s)
    if idx.empty:
        raise AirflowSkipException(f"[misp_osint] Index empty at {base_url}")

    # 2) filter to ds window (UTC) with ±3h tolerance to absorb listing TZ quirks
    ds_start = pd.Timestamp(ds, tz="UTC")
    start = ds_start - pd.Timedelta(hours=3)
    end   = ds_start + pd.Timedelta(days=1, hours=3)
    day_df = idx[(idx["last_modified"] >= start) & (idx["last_modified"] < end)]
    if day_df.empty:
        raise AirflowSkipException(f"[misp_osint] No files in window {start}..{end} for ds={ds}")

    # cap & keep newest within window
    day_df = day_df.sort_values("last_modified").tail(max_files)

    # 3) fetch & extract
    all_rows = []
    errors = 0
    for url in day_df["url"]:
        try:
            r = s.get(url, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            all_rows.extend(_extract_attributes(data))
        except Exception as e:
            errors += 1
            log.warning("[misp_osint] error on %s: %s", url, e)
        time.sleep(rate_sec)

    df = pd.DataFrame(all_rows)

    # Minimal normalization
    if not df.empty:
        # stringify list-like columns
        if "tags" in df.columns:
            df["tags"] = df["tags"].map(lambda v: v if isinstance(v, list) else ([] if pd.isna(v) else [str(v)]))
        if "galaxies" in df.columns:
            df["galaxies"] = df["galaxies"].map(lambda v: v if isinstance(v, list) else ([] if pd.isna(v) else [str(v)]))
        df["tags"] = df["tags"].map(lambda L: ", ".join(map(str, L)) if isinstance(L, list) else "")
        df["galaxies"] = df["galaxies"].map(lambda L: ", ".join(map(str, L)) if isinstance(L, list) else "")
        # drop empties / dupes
        df = df.dropna(subset=["indicator"]).drop_duplicates(subset=["indicator","event_id"])

    n = len(df)
    outdir = f"{SILVER_BASE}/ingest_date={ds}"
    outpath = f"{outdir}/misp_osint.parquet"

    # 4) MIN_ROWS guard (preserve same-day partition if it already exists)
    if n < MIN_ROWS:
        if os.path.exists(outpath):
            raise AirflowSkipException(
                f"[misp_osint] rows={n} < {MIN_ROWS}; keeping existing partition for {ds} at {outpath}"
            )
        raise ValueError(f"[misp_osint] returned {n} rows (<{MIN_ROWS})")

    log.info("[misp_osint] ds=%s index_rows=%d matched_window=%d rows_extracted=%d file_errors=%d",
             ds, len(idx), len(day_df), n, errors)

    # Idempotent + atomic write (no rmtree; single file swap)
    _atomic_to_parquet(df, outpath)
    log.info("[misp_osint] wrote %d rows -> %s", n, outpath)

# ---------------- DAG ----------------

default_args = {"owner": "you", "retries": 3, "retry_delay": __import__("datetime").timedelta(minutes=5)}

with DAG(
    dag_id="misp_osint_ingest",
    start_date=datetime(2025, 11, 3),
    schedule_interval=None,   # orchestrator triggers with ds/ts
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["silver","misp","osint"],
) as dag:
    PythonOperator(
        task_id="misp_osint_fetch",
        python_callable=ingest_misp_osint,
    )
