"""
Report Generator — produces human-readable threat reports.

Tier 1: Perplexity Sonar (web-grounded, with citations)
Tier 2: Template fallback (offline, zero cost)
"""
from __future__ import annotations
import json
import logging
import os
from typing import Optional

from models import InvestigationResult

log = logging.getLogger(__name__)

PERPLEXITY_API_KEY = os.environ.get("PERPLEXITY_API_KEY", "")
PERPLEXITY_MODEL = os.environ.get("PERPLEXITY_MODEL", "sonar")

SYSTEM_PROMPT = """You are a senior cybersecurity threat intelligence analyst.
You are writing an investigation report for an IT administrator who is NOT a security expert.

RULES:
- Start with a one-line verdict: ✅ SAFE / ⚠️ SUSPICIOUS / 🔴 HIGH RISK / 🚨 CRITICAL
- Cite SPECIFIC values from the evidence (actual domain age, actual IPs, actual registrar)
- Explain WHY each factor matters in plain English
- If you find additional context from the web (VirusTotal reports, abuse databases, community forums), include it with source references
- End with exactly 3 numbered recommended actions for the IT admin
- Be concise — max 250 words
- Write in third person ("This domain..." not "I found...")
- If evidence is missing or inconclusive, say so explicitly — never guess
- Format using markdown (headers, bold, bullet points)"""


def _build_evidence_prompt(result: InvestigationResult) -> str:
    """Convert investigation findings to a structured evidence block."""
    dns = result.findings.dns
    whois = result.findings.whois
    threat_db = result.findings.threat_db
    shap = result.findings.shap

    sections = [f"## Domain: {result.domain}\n"]

    # Risk Score
    if result.risk_score is not None:
        sections.append(f"**ML Risk Score:** {result.risk_score:.3f} ({result.risk_level})")

    # DNS
    sections.append("\n### DNS Resolution")
    if dns.error:
        sections.append(f"DNS lookup failed: {dns.error}")
    else:
        sections.append(f"- IPs: {', '.join(dns.ips) if dns.ips else 'None resolved'}")
        sections.append(f"- Countries: {', '.join(dns.countries) if dns.countries else 'Unknown'}")
        sections.append(f"- ASNs: {', '.join(dns.asns) if dns.asns else 'Unknown'}")
        sections.append(f"- ISPs: {', '.join(dns.isps) if dns.isps else 'Unknown'}")
        sections.append(f"- IPv6: {'Yes' if dns.has_ipv6 else 'No'}")

    # WHOIS
    sections.append("\n### WHOIS Registration")
    if whois.error:
        sections.append(f"WHOIS lookup failed: {whois.error}")
    else:
        sections.append(f"- Registrar: {whois.registrar or 'Unknown'}")
        sections.append(f"- Created: {whois.created_date or 'Unknown'}")
        sections.append(f"- Domain Age: {whois.age_days} days" if whois.age_days is not None else "- Domain Age: Unknown")
        sections.append(f"- Expiry: {whois.expiry_date or 'Unknown'}")
        sections.append(f"- Status: {whois.status or 'Unknown'}")

    # Threat DB
    sections.append("\n### Threat Database Check")
    if threat_db.found:
        sections.append(f"- **FOUND** in local threat database")
        sections.append(f"- Sources: {', '.join(threat_db.sources)}")
        sections.append(f"- Labels: {', '.join(threat_db.labels)}")
        if threat_db.first_seen:
            sections.append(f"- First seen: {threat_db.first_seen}")
    else:
        sections.append("- Not found in local threat database")

    # SHAP
    if shap.top_risk_factors:
        sections.append("\n### ML Feature Analysis (SHAP)")
        for factor in shap.top_risk_factors:
            sections.append(f"- {factor}")

    return "\n".join(sections)


def generate_report_sonar(result: InvestigationResult) -> tuple[str, list[str]]:
    """Generate a threat report using Perplexity Sonar with web search."""
    try:
        from openai import OpenAI

        client = OpenAI(
            api_key=PERPLEXITY_API_KEY,
            base_url="https://api.perplexity.ai",
        )

        evidence = _build_evidence_prompt(result)
        user_msg = (
            f"Investigate the domain **{result.domain}** and write a threat assessment report.\n\n"
            f"Here is the evidence gathered from our automated investigation:\n\n{evidence}\n\n"
            f"Search the web for any additional information about this domain — "
            f"check VirusTotal, AbuseIPDB, URLhaus, security forums, and any recent reports. "
            f"Include what you find with source references."
        )

        response = client.chat.completions.create(
            model=PERPLEXITY_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            max_tokens=1024,
            temperature=0.1,
        )

        report = response.choices[0].message.content or ""

        # Extract citations if available
        citations = []
        if hasattr(response, "citations") and response.citations:
            citations = list(response.citations)

        return report, citations

    except Exception as e:
        log.error("Perplexity Sonar failed: %s", e)
        return "", []


def generate_report_template(result: InvestigationResult) -> str:
    """Fallback: generate a structured report using templates."""
    dns = result.findings.dns
    whois = result.findings.whois
    threat_db = result.findings.threat_db
    shap = result.findings.shap

    # Determine verdict
    if result.risk_score is not None:
        if result.risk_score >= 0.90:
            verdict = "🚨 **CRITICAL RISK** — This domain is almost certainly malicious."
        elif result.risk_score >= 0.60:
            verdict = "🔴 **HIGH RISK** — This domain shows strong indicators of malicious activity."
        elif result.risk_score >= 0.20:
            verdict = "⚠️ **SUSPICIOUS** — This domain has some concerning characteristics."
        else:
            verdict = "✅ **LOW RISK** — This domain appears relatively safe based on available evidence."
    else:
        verdict = "❓ **INCONCLUSIVE** — Unable to score this domain. ML model not available."

    lines = [
        f"## Threat Assessment: {result.domain}\n",
        verdict,
        "",
    ]

    # Key findings
    lines.append("### Key Findings\n")

    if threat_db.found:
        lines.append(f"- **Known threat**: Found in {', '.join(threat_db.sources)} database(s) "
                      f"with label(s): {', '.join(threat_db.labels)}")

    if whois.age_days is not None:
        if whois.age_days < 30:
            lines.append(f"- **Very new domain**: Registered only {whois.age_days} days ago "
                          f"via {whois.registrar or 'unknown registrar'}. "
                          f"Newly registered domains are 10x more likely to be malicious.")
        elif whois.age_days < 365:
            lines.append(f"- **Relatively new domain**: {whois.age_days} days old, "
                          f"registered via {whois.registrar or 'unknown registrar'}.")
        else:
            lines.append(f"- **Established domain**: {whois.age_days} days old "
                          f"({whois.age_days // 365} years). Older domains are generally safer.")

    if dns.ips:
        lines.append(f"- **DNS**: Resolves to {len(dns.ips)} IP(s) in "
                      f"{', '.join(dns.countries) if dns.countries else 'unknown location(s)'}.")
    elif dns.error:
        lines.append(f"- **DNS**: Resolution failed — domain may be inactive or using DNS evasion.")
    else:
        lines.append(f"- **DNS**: No IP records found.")

    if shap.top_risk_factors:
        lines.append("\n### ML Explanation (Top Risk Factors)\n")
        for factor in shap.top_risk_factors[:5]:
            lines.append(f"- {factor}")

    # Recommendations
    lines.append("\n### Recommended Actions\n")
    if result.risk_score and result.risk_score >= 0.60:
        lines.extend([
            "1. **Block this domain** on your firewall and DNS filter immediately.",
            "2. **Check email logs** for any messages containing links to this domain.",
            "3. **Alert your team** — if anyone clicked a link to this domain, initiate incident response.",
        ])
    elif result.risk_score and result.risk_score >= 0.20:
        lines.extend([
            "1. **Monitor this domain** — add it to your watchlist for the next 7 days.",
            "2. **Warn users** not to interact with this domain until classification is confirmed.",
            "3. **Re-scan in 48 hours** — new domains may accumulate more threat intelligence data.",
        ])
    else:
        lines.extend([
            "1. **No immediate action required** — this domain appears safe.",
            "2. **Standard caution** — always verify before sharing credentials on any site.",
            "3. **Re-scan periodically** — threat status can change over time.",
        ])

    return "\n".join(lines)


def generate_report(result: InvestigationResult) -> InvestigationResult:
    """
    Generate a threat report. Tries Perplexity Sonar first, falls back to template.
    Mutates and returns the result with ai_report, citations, recommendations filled.
    """
    if PERPLEXITY_API_KEY:
        log.info("Generating report via Perplexity Sonar for %s", result.domain)
        report, citations = generate_report_sonar(result)
        if report:
            result.ai_report = report
            result.citations = citations
            result.agent_mode = "full"
            # Extract recommendations from report if possible
            result.recommendations = _extract_recommendations(report)
            return result
        log.warning("Sonar failed, falling back to template for %s", result.domain)

    # Fallback
    log.info("Generating template report for %s", result.domain)
    result.ai_report = generate_report_template(result)
    result.agent_mode = "template"
    result.recommendations = _extract_recommendations(result.ai_report)
    return result


def _extract_recommendations(report: str) -> list[str]:
    """Pull numbered recommendations from a markdown report."""
    recs = []
    for line in report.split("\n"):
        line = line.strip()
        if line and line[0].isdigit() and line[1] in ".)" and "**" in line:
            # Clean up markdown bold
            clean = line[2:].strip().lstrip("*").rstrip("*").strip()
            recs.append(clean)
    return recs[:3]
