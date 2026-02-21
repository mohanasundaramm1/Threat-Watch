import json
from jsonschema import validate, Draft202012Validator

# Define your schema (same shape your producer uses)
schema = {
    "type": "object",
    "required": ["id", "domain", "event_ts", "source", "producer_ts"],
    "properties": {
        "id": {"type": "string"},
        "domain": {"type": "string"},
        "tld": {"type": ["string", "null"]},
        "event_ts": {"type": "string", "format": "date-time"},
        "source": {"type": "string"},
        "producer_ts": {"type": "string", "format": "date-time"}
    }
}

def test_sample_event_validates():
    sample = {
        "id": "1234-uuid",
        "domain": "example.com",
        "tld": "com",
        "event_ts": "2025-09-02T00:00:00Z",
        "source": "simulator",
        "producer_ts": "2025-09-02T00:00:01Z"
    }
    Draft202012Validator(schema).validate(sample)

