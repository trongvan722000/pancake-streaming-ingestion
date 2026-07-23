import json
import logging
import os
import uuid

from fastapi import FastAPI, HTTPException, Request, Response

from . import producer as kafka_producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pancake-gateway")

WEBHOOK_SECRET = os.environ["PANCAKE_WEBHOOK_SECRET"]

app = FastAPI()


@app.get("/webhooks/pancake/{secret_token}")
def verify_webhook(secret_token: str):
    if secret_token != WEBHOOK_SECRET:
        raise HTTPException(status_code=404)
    return Response(status_code=200)


@app.post("/webhooks/pancake/{secret_token}")
async def receive_webhook(secret_token: str, request: Request):
    if secret_token != WEBHOOK_SECRET:
        raise HTTPException(status_code=404)

    request_id = str(uuid.uuid4())
    raw_body = await request.body()

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        logger.warning("request_id=%s invalid json body, storing raw text", request_id)
        payload = {"_raw": raw_body.decode("utf-8", errors="replace")}

    kafka_producer.produce_event(payload, request_id)

    return {"status": "ok", "request_id": request_id}


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.on_event("shutdown")
def _flush_producer():
    kafka_producer.flush(10)
