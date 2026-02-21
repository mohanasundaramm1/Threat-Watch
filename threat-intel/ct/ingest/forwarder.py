import os, json, time, re
from datetime import datetime, timezone
from websocket import WebSocketApp
from kafka import KafkaProducer

# Kafka / Certstream config
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC     = os.getenv("KAFKA_TOPIC", "ct-events")
WS_URL    = os.getenv("CERTSTREAM_WS", "ws://127.0.0.1:4000")
SAMPLE_EVERY_N = int(os.getenv("SAMPLE_EVERY_N", "1"))  # 1 = no sampling

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8"),
    linger_ms=50,
    acks="all",
    retries=5,
    request_timeout_ms=30000,
)

domain_re = re.compile(r"^[A-Za-z0-9\-\._*]+$")

def clean_domain(d: str) -> str:
    if not isinstance(d, str):
        return ""
    d = d.strip().lower().rstrip(".")
    if d.startswith("*."):
        d = d[2:]
    return d

def evt(domain: str, offset: int = 0):
    # RFC3339 UTC with 'Z' so Spark can parse easily
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "id": f"ct-{int(time.time() * 1000)}-{offset}",
        "domain": domain,
        "tld": domain.split(".")[-1] if "." in domain else None,
        "event_ts": now,
        "producer_ts": now,
        "source": "certstream",
    }

def on_message(_ws, msg):
    try:
        data = json.loads(msg)
        leaf = (data.get("data") or {}).get("leaf_cert") or {}
        doms = leaf.get("all_domains") or []
        if not doms:
            return

        sent = 0
        for i, raw in enumerate(doms):
            if (i % SAMPLE_EVERY_N) != 0:
                continue
            d = clean_domain(raw)
            if not d or not domain_re.match(d):
                continue
            producer.send(TOPIC, key=d, value=evt(d, i))
            sent += 1
        if sent:
            producer.flush(0)
    except Exception as e:
        print("parse/send error:", repr(e))

def on_open(_):   print(f"[ct] ws {WS_URL} → kafka {BOOTSTRAP} topic {TOPIC}")
def on_error(_, e): print("[ct] ws error:", e)
def on_close(*_): print("[ct] ws closed")

if __name__ == "__main__":
    while True:
        try:
            ws = WebSocketApp(
                WS_URL,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.on_open = on_open
            ws.run_forever(ping_interval=30, ping_timeout=20)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print("[ct] websocket crashed, retrying in 5s:", repr(e))
            time.sleep(5)
