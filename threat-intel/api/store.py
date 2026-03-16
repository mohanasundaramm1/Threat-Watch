"""SQLite-backed investigation history store."""
from __future__ import annotations
import json
import logging
import os
import sqlite3
from typing import Optional

from models import InvestigationResult

log = logging.getLogger(__name__)

DB_PATH = os.environ.get("INVESTIGATION_DB", "/data/investigations.db")


def _get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS investigations (
            id TEXT PRIMARY KEY,
            domain TEXT NOT NULL,
            risk_score REAL,
            risk_level TEXT,
            ai_report TEXT,
            agent_mode TEXT,
            result_json TEXT NOT NULL,
            investigated_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_domain ON investigations(domain)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_investigated_at ON investigations(investigated_at DESC)
    """)
    conn.commit()
    return conn


def save(result: InvestigationResult) -> None:
    """Persist an investigation result."""
    try:
        conn = _get_conn()
        conn.execute(
            """INSERT OR REPLACE INTO investigations
               (id, domain, risk_score, risk_level, ai_report, agent_mode, result_json, investigated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                result.id,
                result.domain,
                result.risk_score,
                result.risk_level,
                result.ai_report,
                result.agent_mode,
                result.model_dump_json(),
                result.investigated_at,
            ),
        )
        conn.commit()
        conn.close()
        log.info("Saved investigation %s for %s", result.id, result.domain)
    except Exception as e:
        log.error("Failed to save investigation: %s", e)


def get(investigation_id: str) -> Optional[InvestigationResult]:
    """Retrieve investigation by ID."""
    try:
        conn = _get_conn()
        row = conn.execute(
            "SELECT result_json FROM investigations WHERE id = ?",
            (investigation_id,)
        ).fetchone()
        conn.close()
        if row:
            return InvestigationResult.model_validate_json(row[0])
    except Exception as e:
        log.error("Failed to get investigation %s: %s", investigation_id, e)
    return None


def list_recent(limit: int = 50, offset: int = 0) -> list[dict]:
    """List recent investigations (summary only)."""
    try:
        conn = _get_conn()
        rows = conn.execute(
            """SELECT id, domain, risk_score, risk_level, agent_mode, investigated_at
               FROM investigations
               ORDER BY investigated_at DESC
               LIMIT ? OFFSET ?""",
            (limit, offset)
        ).fetchall()
        conn.close()
        return [
            {
                "id": r[0],
                "domain": r[1],
                "risk_score": r[2],
                "risk_level": r[3],
                "agent_mode": r[4],
                "investigated_at": r[5],
            }
            for r in rows
        ]
    except Exception as e:
        log.error("Failed to list investigations: %s", e)
        return []
