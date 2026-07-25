import os
import random
import string
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI

app = FastAPI(title="pancake-simulator")

GATEWAY_URL = os.environ["GATEWAY_URL"]  # URL webhook đầy đủ, kèm secret token ở cuối path

EVENT_TYPES = ["messaging", "conversation", "subscription", "post", "connect_status"]
SENDERS = ["van", "vy", "khach_hang_01"]
RECEIVERS = ["page_fanpage", "zalo_oa"]


def _random_id(prefix: str) -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"{prefix}_{suffix}"


def _fake_event() -> dict:
    return {
        "type": random.choice(EVENT_TYPES),
        "conversation_id": _random_id("conv"),
        "sender": random.choice(SENDERS),
        "receiver": random.choice(RECEIVERS),
        "content": f"tin nhan gia lap luc {datetime.now(timezone.utc).isoformat()}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.post("/simulate")
def simulate(count: int = 1):
    results = []
    with httpx.Client(timeout=5.0) as client:
        for _ in range(count):
            event = _fake_event()
            resp = client.post(GATEWAY_URL, json=event)
            results.append({"event": event, "status_code": resp.status_code})
    return {"sent": count, "results": results}
