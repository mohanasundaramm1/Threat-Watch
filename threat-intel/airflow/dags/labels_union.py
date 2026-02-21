# airflow/dags/labels_union_dag.py
from __future__ import annotations
import os, tempfile, logging
from datetime import datetime

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

log = logging.getLogger(__name__)

SILVER_URLHAUS   = "/opt/airflow/silver/urlhaus"
SILVER_OPENPHISH = "/opt/airflow/silver/openphish"
UNION_OUT        = "/opt/airflow/silver/labels_union"

# ---------------- helpers ----------------
def _effective_ds_ts(ds: str, ts: str, **context):
    """Use ds/ts from dag_run.conf if orchestrator passed them."""
    dr = context.get("dag_run")
    if dr and getattr(dr, "conf", None):
        ds_conf = dr.conf.get("ds") or ds
        ts_conf = dr.conf.get("ts") or ts
        if ds_conf != ds or ts_conf != ts:
            log.info("Overriding ds/ts from dag_run.conf: %s,%s -> %s,%s",
                     ds, ts, ds_conf, ts_conf)
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
    """Write to tmp then atomic rename to avoid partial files."""
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(suffix=".parquet", dir=os.path.dirname(final_path) or ".")
    os.close(fd)
    df.to_parquet(tmp, index=False)
    os.replace(tmp, final_path)

def _read_if_exists(path: str) -> pd.DataFrame | None:
    if not os.path.exists(path):
        return None
    df = pd.read_parquet(path)

    # Enforce expected schema & dtypes (tolerant to missing columns)
    out = pd.DataFrame({
        "domain": df.get("domain", pd.Series(dtype="string")).astype("string"),
        "url": df.get("url", pd.Series(dtype="string")).astype("string"),
        "first_seen": pd.to_datetime(df.get("first_seen"), utc=True, errors="coerce"),
        "label": df.get("label", pd.Series(dtype="string")).astype("string"),
        "source": df.get("source", pd.Series(dtype="string")).astype("string"),
    })
    return out

# ---------------- task ----------------
def build_union(ds: str | None = None, ts: str | None = None, **context):
    ds, ts = _resolve_ds_ts_flexible(ds, ts, **context)

    uh_p = f"{SILVER_URLHAUS}/ingest_date={ds}/urlhaus_silver.parquet"
    op_p = f"{SILVER_OPENPHISH}/ingest_date={ds}/openphish_silver.parquet"

    uh = _read_if_exists(uh_p)
    op = _read_if_exists(op_p)

    dfs: list[pd.DataFrame] = []
    if uh is not None and len(uh):
        uh = uh.assign(seen_in_urlhaus=True, seen_in_openphish=False)
        dfs.append(uh)
    if op is not None and len(op):
        op = op.assign(seen_in_urlhaus=False, seen_in_openphish=True)
        dfs.append(op)

    if dfs:
        union = pd.concat(dfs, ignore_index=True)
    else:
        # Empty-but-valid dataset prevents downstream errors
        union = pd.DataFrame({
            "domain": pd.Series(dtype="string"),
            "url": pd.Series(dtype="string"),
            "first_seen": pd.Series(dtype="datetime64[ns, UTC]"),
            "label": pd.Series(dtype="string"),
            "source": pd.Series(dtype="string"),
            "seen_in_urlhaus": pd.Series(dtype="bool"),
            "seen_in_openphish": pd.Series(dtype="bool"),
        })
        log.warning("[labels_union] %s: no silver inputs found; writing empty union.", ds)

    # Normalize & hygiene
    union["domain"] = union["domain"].astype("string")
    union["url"] = union["url"].astype("string")
    union["first_seen"] = pd.to_datetime(union["first_seen"], utc=True, errors="coerce")
    union["label"] = union["label"].astype("string")
    union["source"] = union["source"].astype("string")
    union["seen_in_urlhaus"] = union.get("seen_in_urlhaus", False).fillna(False).astype(bool)
    union["seen_in_openphish"] = union.get("seen_in_openphish", False).fillna(False).astype(bool)

    union = union.dropna(subset=["domain", "first_seen"])

    # Dedupe: include URL so distinct URLs on same domain/timestamp aren't collapsed
    union = union.sort_values(["domain", "first_seen", "source", "url"]).drop_duplicates(
        subset=["domain", "first_seen", "source", "url"], keep="first"
    )

    # Idempotent write to ds partition (atomic file replace)
    outdir = f"{UNION_OUT}/ingest_date={ds}"
    out_path = f"{outdir}/labels_union.parquet"
    _atomic_to_parquet(union, out_path)
    log.info("[labels_union] %s: wrote %d rows -> %s", ds, len(union), out_path)

default_args = {"owner": "you", "retries": 0}

with DAG(
    dag_id="labels_union",
    start_date=datetime(2025, 11, 3),
    schedule_interval=None,   # orchestrator triggers with ds/ts
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["silver", "view"],
) as dag:
    PythonOperator(
        task_id="build_union",
        python_callable=build_union,
    )
