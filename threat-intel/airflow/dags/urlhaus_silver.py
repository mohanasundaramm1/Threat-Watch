# airflow/dags/urlhaus_silver_dag.py
from __future__ import annotations
import os, shutil, tempfile, logging
from datetime import datetime

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.exceptions import AirflowSkipException

log = logging.getLogger(__name__)

BRONZE = "/opt/airflow/bronze/urlhaus"
SILVER = "/opt/airflow/silver/urlhaus"

def _effective_ds_ts(ds: str, ts: str, **context):
    """Use ds/ts from dag_run.conf if the orchestrator passed them."""
    dr = context.get("dag_run")
    if dr and getattr(dr, "conf", None):
        ds_conf = dr.conf.get("ds") or ds
        ts_conf = dr.conf.get("ts") or ts
        if ds_conf != ds or ts_conf != ts:
            log.info(
                "Overriding ds/ts from dag_run.conf: %s,%s -> %s,%s",
                ds, ts, ds_conf, ts_conf
            )
        return ds_conf, ts_conf
    return ds, ts

def _resolve_ds_ts_flexible(ds: str | None, ts: str | None, **maybe_ctx):
    """
    Works whether ds/ts/context were provided explicitly or not.
    Falls back to Airflow runtime context; last-resort fallback = 'now' (UTC).
    """
    try:
        ctx = maybe_ctx if ("dag_run" in maybe_ctx) else get_current_context()
        ds0 = ds or ctx["ds"]
        ts0 = ts or ctx["ts"]
        return _effective_ds_ts(ds0, ts0, **ctx)
    except Exception:
        now = pd.Timestamp.utcnow()
        return (now.date().isoformat(), now.strftime("%Y-%m-%d %H:%M:%S"))

def _atomic_to_parquet(df: pd.DataFrame, final_path: str):
    """Write to a temp file then atomically replace the final file."""
    fd, tmp_path = tempfile.mkstemp(suffix=".parquet", dir=os.path.dirname(final_path) or ".")
    os.close(fd)
    df.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, final_path)

def urlhaus_to_silver(ds: str | None = None, ts: str | None = None, **context):
    # Align with orchestrator’s logical date/time if provided
    ds, ts = _resolve_ds_ts_flexible(ds, ts, **context)

    bronze_path = f"{BRONZE}/ingest_date={ds}/urlhaus.parquet"

    # If bronze partition doesn't exist, SKIP (don’t fail the day)
    if not os.path.exists(bronze_path):
        raise AirflowSkipException(f"[urlhaus_silver] no bronze for {ds}: {bronze_path}")

    # Read; if empty, skip
    df = pd.read_parquet(bronze_path)
    if df is None or len(df) == 0:
        raise AirflowSkipException(f"[urlhaus_silver] bronze empty for {ds}: {bronze_path}")

    # Ensure required columns exist (safe defaults sized to df)
    if "domain" not in df.columns:
        df["domain"] = pd.Series([None] * len(df), dtype="object")
    if "url" not in df.columns:
        df["url"] = pd.Series([None] * len(df), dtype="object")
    if "first_seen" not in df.columns:
        df["first_seen"] = pd.Series([None] * len(df), dtype="object")
    if "threat" not in df.columns:
        df["threat"] = pd.Series(["unknown"] * len(df), dtype="object")

    # Normalize to stable schema
    out = pd.DataFrame({
        "domain": df["domain"].astype("string"),
        "url": df["url"].astype("string"),
        "first_seen": pd.to_datetime(df["first_seen"], utc=True, errors="coerce"),
        "label": df["threat"].astype("string").fillna("unknown"),
        "source": "urlhaus",  # scalar broadcasts
    })

    # Hygiene & dedupe
    out = out.dropna(subset=["domain", "first_seen"])
    out = out.sort_values(["domain", "first_seen"]).drop_duplicates(
        subset=["domain", "first_seen"], keep="first"
    )

    # Idempotent, atomic write to ds partition
    outdir = f"{SILVER}/ingest_date={ds}"
    if os.path.exists(outdir):
        shutil.rmtree(outdir)
    os.makedirs(outdir, exist_ok=True)

    final_path = f"{outdir}/urlhaus_silver.parquet"
    _atomic_to_parquet(out, final_path)

    log.info("[urlhaus_silver] %s: wrote %d rows -> %s", ds, len(out), final_path)

default_args = {"owner": "you", "retries": 0}

with DAG(
    dag_id="urlhaus_silver",
    start_date=datetime(2025, 11, 3),
    schedule=None,          # <— disable its own schedule; orchestrator triggers it
    catchup=False,          # orchestrator controls dates; no backfill here
    max_active_runs=1,
    default_args=default_args,
    tags=["silver", "normalize"],
) as dag:
    PythonOperator(
        task_id="urlhaus_bronze_to_silver",
        python_callable=urlhaus_to_silver,
    )
