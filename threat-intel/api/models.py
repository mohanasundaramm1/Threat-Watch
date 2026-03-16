"""Pydantic models for the Investigation API."""
from __future__ import annotations
from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime
import re


class InvestigationRequest(BaseModel):
    """Input model — the domain to investigate."""
    domain: str = Field(..., min_length=3, max_length=253, examples=["example.com"])

    @field_validator("domain")
    @classmethod
    def clean_domain(cls, v: str) -> str:
        v = v.strip().lower()
        # Strip protocol if user pastes a full URL
        v = re.sub(r"^https?://", "", v)
        v = v.split("/")[0]  # drop path
        v = v.split("?")[0]  # drop query
        if not re.match(r"^[a-z0-9]([a-z0-9\-\.]*[a-z0-9])?$", v):
            raise ValueError(f"Invalid domain format: {v}")
        return v


class DNSFindings(BaseModel):
    ips: list[str] = []
    countries: list[str] = []
    asns: list[str] = []
    isps: list[str] = []
    has_ipv6: bool = False
    ip_count: int = 0
    error: Optional[str] = None


class WHOISFindings(BaseModel):
    registrar: Optional[str] = None
    created_date: Optional[str] = None
    expiry_date: Optional[str] = None
    age_days: Optional[int] = None
    status: Optional[str] = None
    name_servers: list[str] = []
    error: Optional[str] = None


class ThreatDBFindings(BaseModel):
    found: bool = False
    sources: list[str] = []
    labels: list[str] = []
    first_seen: Optional[str] = None
    risk_score_from_db: Optional[float] = None


class SHAPExplanation(BaseModel):
    """Top feature contributions from SHAP."""
    feature_contributions: dict[str, float] = {}  # feature_name → SHAP value
    top_risk_factors: list[str] = []               # human-readable top reasons


class InvestigationFindings(BaseModel):
    dns: DNSFindings = Field(default_factory=DNSFindings)
    whois: WHOISFindings = Field(default_factory=WHOISFindings)
    threat_db: ThreatDBFindings = Field(default_factory=ThreatDBFindings)
    shap: SHAPExplanation = Field(default_factory=SHAPExplanation)


class InvestigationResult(BaseModel):
    """Full investigation output."""
    id: str
    domain: str
    risk_score: Optional[float] = None
    risk_level: Optional[str] = None  # LOW / MEDIUM / HIGH / CRITICAL
    findings: InvestigationFindings = Field(default_factory=InvestigationFindings)
    ai_report: str = ""              # Markdown report from LLM
    citations: list[str] = []        # Source URLs from Perplexity
    recommendations: list[str] = []
    investigated_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    latency_seconds: Optional[float] = None
    agent_mode: str = "full"         # "full" | "template" | "offline"
