import os, time, uuid, json, random, string
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC     = os.getenv("KAFKA_TOPIC", "domain-events")
RATE_SECS = float(os.getenv("PRODUCER_RATE_SECS", "1.0"))
HEARTBEAT_SECS = float(os.getenv("PRODUCER_HEARTBEAT_SECS", "30"))

TLDs = ["com","net","org","xyz","info","top","site","online","shop","live","click","icu"]
ADJ  = ["fast","secure","login","verify","payment","support","account","update","wallet","drop"]
BRAND= ["paypal","apple","google","amazon","steam","microsoft","bank","irs","dhl","ups","tax"]

def rand_domain():
    use_brand = random.random() < 0.6
    adj = random.choice(ADJ)
    brand = random.choice(BRAND) if use_brand else ''.join(random.choices(string.ascii_lowercase, k=random.randint(5,10)))
    tld = random.choice(TLDs)
    name = f"{brand}{'-' if random.random()<0.5 else ''}{adj}"
    return f"{name}.{tld}", tld

def main():
    print(f"Connecting to {BOOTSTRAP} topic {TOPIC} ...")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
        linger_ms=50, acks="all", retries=5,
    )
    sent, last_hb = 0, time.time()
    while True:
        domain, tld = rand_domain()
        now = datetime.now(timezone.utc).isoformat()
        evt = {
            "id": str(uuid.uuid4()),
            "domain": domain,
            "tld": tld,
            "event_ts": now,
            "source": "simulator",
            "producer_ts": now,
        }
        producer.send(TOPIC, key=domain, value=evt)
        sent += 1
        if time.time() - last_hb >= HEARTBEAT_SECS:
            print(f"[heartbeat] sent_total={sent} rate≈{round(1.0/max(RATE_SECS,1e-9),1)} msg/s")
            last_hb = time.time()
        time.sleep(RATE_SECS)

if __name__ == "__main__":
    main()
