"""FastAPI server for the Investigation Agent."""
from __future__ import annotations
import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from models import InvestigationRequest, InvestigationResult
from investigate import InvestigationAgent
from report_generator import generate_report
import store

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

agent = InvestigationAgent()


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Investigation API starting up")
    yield
    log.info("Investigation API shutting down")


app = FastAPI(
    title="ThreatWatch Investigation API",
    description="Agentic threat investigation engine with ML scoring and SHAP explainability",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/api/v1/investigate", response_model=InvestigationResult)
def investigate_domain(req: InvestigationRequest):
    """Run a full autonomous investigation on a domain."""
    log.info("Investigation request for: %s", req.domain)

    try:
        # Step 1: Gather evidence + ML scoring
        result = agent.investigate(req.domain)

        # Step 2: Generate AI report (Sonar or template)
        result = generate_report(result)

        # Step 3: Persist to history
        store.save(result)

        log.info("Investigation complete for %s: score=%s, level=%s, mode=%s, latency=%.2fs",
                 result.domain, result.risk_score, result.risk_level,
                 result.agent_mode, result.latency_seconds or 0)

        return result

    except Exception as e:
        log.error("Investigation failed for %s: %s", req.domain, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Investigation failed: {str(e)}")


@app.get("/api/v1/investigate/{investigation_id}", response_model=InvestigationResult)
def get_investigation(investigation_id: str):
    """Retrieve a past investigation by ID."""
    result = store.get(investigation_id)
    if not result:
        raise HTTPException(status_code=404, detail="Investigation not found")
    return result


@app.get("/api/v1/investigate/history")
def list_investigations(limit: int = 50, offset: int = 0):
    """List recent investigations."""
    return store.list_recent(limit=min(limit, 100), offset=offset)


@app.get("/health")
def health():
    return {"status": "ok", "service": "investigation-api"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
