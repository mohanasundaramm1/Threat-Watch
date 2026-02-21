# airflow/dags/urlhaus_ingest.py
from __future__ import annotations
import io, os, re, time, shutil, tempfile, logging
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
import requests
import great_expectations as ge

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.exceptions import AirflowSkipException

log = logging.getLogger(__name__)

BRONZE_BASE = "/opt/airflow/bronze/urlhaus"
MIN_ROWS = 50
FEED_MAX_STALENESS_HOURS = 12  # optional guardrail

def _effective_ds_ts(ds: str, ts: str, **context):
    dr = context.get("dag_run")
    if dr and getattr(dr, "conf", None):
        ds2 = dr.conf.get("ds") or ds
        ts2 = dr.conf.get("ts") or ts
        if ds2 != ds or ts2 != ts:
            log.info("Overriding ds/ts from dag_run.conf: %s,%s -> %s,%s", ds, ts, ds2, ts2)
        return ds2, ts2
    return ds, ts

def _resolve_ds_ts_flexible(ds: str | None, ts: str | None, **maybe_ctx):
    """Works whether ds/ts/context were provided explicitly or not."""
    try:
        ctx = maybe_ctx if ("dag_run" in maybe_ctx) else get_current_context()
        ds0 = ds or ctx["ds"]
        ts0 = ts or ctx["ts"]
        return _effective_ds_ts(ds0, ts0, **ctx)
    except Exception:
        # last-resort fallback for tests/non-Airflow execution
        now = pd.Timestamp.utcnow()
        return (now.date().isoformat(), now.strftime("%Y-%m-%d %H:%M:%S"))

def _to_utc_timestamp(ts_str: str):
    t = pd.Timestamp(ts_str)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")

def _host(u: str):
    u = str(u).strip()
    if not re.match(r"^https?://", u):
        u = "http://" + u
    return urlparse(u).hostname

def fetch_and_write_urlhaus(ds: str | None = None, ts: str | None = None, **context):
    # Align with orchestrator’s logical date/time
    ds, ts = _resolve_ds_ts_flexible(ds, ts, **context)

    ds_date = pd.to_datetime(ds).date()
    today_date = pd.Timestamp.utcnow().date()
    outdir = f"{BRONZE_BASE}/ingest_date={ds}"

    # URLhaus csv_online is a *current* snapshot → never synthesize history
    if ds_date != today_date:
        if os.path.exists(outdir):
            raise AirflowSkipException(f"Historical partition exists for {ds}; skipping rewrite.")
        raise AirflowSkipException("URLhaus has no historical feed; skipping backfill.")

    # Resilient fetch
    url = "https://urlhaus.abuse.ch/downloads/csv_online/"
    sess = requests.Session()
    last_exc = None
    for attempt in range(3):
        try:
            r = sess.get(url, timeout=(5, 45), headers={"User-Agent": "threat-intel-lab/0.1"})
            if r.ok and len(r.text) > 1000:
                break
            last_exc = RuntimeError(f"Bad response (status={r.status_code}, len={len(r.text)})")
        except Exception as e:
            last_exc = e
        time.sleep(2)
    else:
        raise last_exc if last_exc else RuntimeError("URLhaus fetch failed")

    # Optional freshness guard (header is commented; parse from text)
    m = re.search(r"Last updated:\s*([0-9\-: ]+)\s*\(UTC\)", r.text)
    if m:
        header_ts = pd.Timestamp(m.group(1), tz="UTC")
        run_ts = _to_utc_timestamp(ts)
        age = (run_ts - header_ts).total_seconds() / 3600.0
        if age > FEED_MAX_STALENESS_HOURS:
            raise ValueError(f"[urlhaus_ingest] feed stale: {age:.1f}h > {FEED_MAX_STALENESS_HOURS}h")

    # Parse CSV (skip '#')
    raw = pd.read_csv(io.StringIO(r.text), comment="#", header=None)
    if raw.shape[1] >= 9:
        df = raw.iloc[:, :9].copy()
        # feed header is: id,dateadded,url,url_status,last_online,threat,tags,urlhaus_link,reporter
        df.columns = ["id","dateadded","url","url_status","last_online","threat","tags","urlhaus_link","reporter"]
        df = df.rename(columns={"url_status": "status"})
    else:
        df = raw.copy()
        df.columns = [f"c{i}" for i in range(df.shape[1])]
        url_col = next(
            (c for c in df.columns if pd.Series(df[c].astype(str)).str.startswith(("http://","https://")).mean() > 0.5),
            df.columns[0]
        )
        df = df.rename(columns={url_col: "url"})
        df["dateadded"] = ds

    # Normalize
    df["domain"] = df["url"].map(_host)
    df["first_seen"] = _to_utc_timestamp(ts)
    cols = ["url","domain","threat","tags","urlhaus_link","reporter","status","last_online","dateadded","first_seen"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols].dropna(subset=["domain"]).drop_duplicates(subset=["url"])

    # Guardrails + GE
    n = len(df)
    if n < MIN_ROWS:
        if os.path.exists(outdir):
            raise AirflowSkipException(
                f"[urlhaus_ingest] rows={n} < {MIN_ROWS}; keeping existing partition for {ds} at {outdir}"
            )
        raise ValueError(f"[urlhaus_ingest] returned {n} rows (<{MIN_ROWS})")

    if not df["url"].is_unique:
        raise ValueError("duplicate URLs leaked after drop_duplicates")
    if not df["domain"].str.len().between(1, 253).all():
        raise ValueError("domain length out of bounds")

    gdf = ge.from_pandas(df)
    assert gdf.expect_column_to_exist("domain").success
    assert gdf.expect_column_values_to_not_be_null("domain").success
    assert gdf.expect_column_values_to_match_regex("url", r"^https?://").success

    # Idempotent + atomic write
    if os.path.exists(outdir):
        shutil.rmtree(outdir)
    os.makedirs(outdir, exist_ok=True)
    final_path = f"{outdir}/urlhaus.parquet"
    fd, tmp = tempfile.mkstemp(suffix=".parquet", dir=outdir); os.close(fd)
    df.to_parquet(tmp, index=False)
    os.replace(tmp, final_path)
    log.info("[urlhaus_ingest] %s: wrote %d rows -> %s", ds, n, final_path)

default_args = {"owner": "you", "retries": 3, "retry_delay": __import__("datetime").timedelta(minutes=5)}

with DAG(
    dag_id="urlhaus_ingest",
    start_date=datetime(2025, 11, 3),
    schedule=None,              # <— let the ORCHESTRATOR trigger this; prevents duplicate runs
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ingest","bronze"],
) as dag:
    PythonOperator(
        task_id="urlhaus_fetch_normalize_bronze",
        python_callable=fetch_and_write_urlhaus,
    )
