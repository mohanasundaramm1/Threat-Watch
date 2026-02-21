from __future__ import annotations
import os, json, shutil
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

# Output location inside the container (mounted to your repo ../silver)
SILVER_BASE = "/opt/airflow/silver/misp_indicators"

# Minimal normalized schema we’ll keep stable
COLUMNS = [
    "indicator",     # string (domain/hostname/url)
    "type",          # "domain" | "hostname" | "url" | ...
    "category",      # optional (MISP attribute category)
    "orgc",          # contributing org (event orgc)
    "event_id",      # event id
    "first_seen",    # timestamp if available
    "last_seen",     # timestamp if available
    "tags",          # list[str] (json-encoded)
    "galaxies",      # list[str] (json-encoded)
    "source",        # always "misp"
]

def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=COLUMNS).assign(
        source="misp"
    )

def _normalize_attributes(attrs: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for a in attrs:
        value = a.get("value")
        atype = a.get("type")
        category = a.get("category")
        event = a.get("Event") or {}
        orgc = (event.get("Orgc") or {}).get("name")
        event_id = event.get("id")
        # timestamps, tags, galaxies (best-effort)
        first_seen = a.get("first_seen") or a.get("timestamp")
        last_seen  = a.get("last_seen")
        # tags may be on attribute or event
        tags_attr = [t.get("name") for t in (a.get("Tag") or []) if "name" in t]
        tags_ev   = [t.get("name") for t in (event.get("Tag") or []) if "name" in t]
        tags = list({*tags_attr, *tags_ev})
        # galaxies (event level most common)
        galaxies = []
        for g in event.get("Galaxy", []) or []:
            for c in g.get("GalaxyCluster", []) or []:
                name = c.get("value") or c.get("tag_name")
                if name: galaxies.append(name)

        rows.append({
            "indicator": value,
            "type": atype,
            "category": category,
            "orgc": orgc,
            "event_id": event_id,
            "first_seen": pd.to_datetime(first_seen, utc=True, errors="coerce"),
            "last_seen":  pd.to_datetime(last_seen,  utc=True, errors="coerce"),
            "tags": json.dumps(tags, ensure_ascii=False),
            "galaxies": json.dumps(galaxies, ensure_ascii=False),
            "source": "misp",
        })
    df = pd.DataFrame(rows, columns=COLUMNS)
    return df

def fetch_and_write_misp(ds: str, **_):
    """
    If MISP_URL or MISP_API_KEY is empty:
      -> write empty parquet for the day (idempotent)
    Else:
      -> call /attributes/restSearch for recent indicators and write results
    """
    url = os.getenv("MISP_URL", "").strip()
    key = os.getenv("MISP_API_KEY", "").strip()
    verify_ssl = os.getenv("MISP_VERIFY_SSL", "true").lower() == "true"

    outdir = f"{SILVER_BASE}/ingest_date={ds}"

    # Always idempotent per-day: remove then recreate partition dir
    if os.path.exists(outdir):
        shutil.rmtree(outdir)
    os.makedirs(outdir, exist_ok=True)

    # No credentials? write empty file with stable schema and return
    if not url or not key:
        df = _empty_df()
        df.to_parquet(f"{outdir}/misp.parquet", index=False)
        print(f"[misp_ingest] No credentials set. Wrote EMPTY partition for ds={ds} at {outdir}.")
        return

    # We have credentials: fetch a small recent window of attributes
    endpoint = url.rstrip("/") + "/attributes/restSearch"
    headers = {
        "Authorization": key,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    # Keep the query small and fast; we only need domains/hostnames/urls
    body = {
        "type": ["domain", "hostname", "url"],
        "published": 1,
        "timestamp": "7d",       # last 7 days
        "limit": 10000,
        # Optional: exclude feeds you already ingest directly (names vary by instance)
        # "tags": ["!feed:URLhaus", "!feed:OpenPhish"]
    }

    try:
        r = requests.post(endpoint, headers=headers, json=body, timeout=120, verify=verify_ssl)
        r.raise_for_status()
        data = r.json()
        attrs = ((data or {}).get("response") or {}).get("Attribute") or []
        print(f"[misp_ingest] fetched attributes: {len(attrs)}")

        if not attrs:
            df = _empty_df()
        else:
            df = _normalize_attributes(attrs)

        df.to_parquet(f"{outdir}/misp.parquet", index=False)
        print(f"[misp_ingest] wrote {len(df)} rows to {outdir}/misp.parquet")

    except Exception as e:
        # Fail soft: write empty partition so downstream doesn’t break
        print(f"[misp_ingest] ERROR: {e}. Writing EMPTY partition for ds={ds}.")
        df = _empty_df()
        df.to_parquet(f"{outdir}/misp.parquet", index=False)

default_args = {"owner": "you", "retries": 3, "retry_delay": __import__("datetime").timedelta(minutes=5)}

with DAG(
    dag_id="misp_ingest",
    start_date=datetime(2025, 11, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ingest","silver","misp"],
) as dag:
    PythonOperator(
        task_id="misp_fetch_normalize_silver",
        python_callable=fetch_and_write_misp,
    )
