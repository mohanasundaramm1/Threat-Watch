# Threat Intel – Local Streaming Skeleton

Kafka → PySpark → Console/Bronze with DLQ, metrics, and tests.

## Prereqs
- macOS, Docker Desktop
- Python 3.11, Java 11
- Install deps:
  ```bash
  python3.11 -m venv .venv
  source .venv/bin/activate
  pip install -r requirements.txt
  cp .env.example .env

