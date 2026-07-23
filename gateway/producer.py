import json
import logging
import os
from typing import Any, Optional

from confluent_kafka import Producer

logger = logging.getLogger("pancake-gateway")

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
RAW_TOPIC = os.environ.get("PANCAKE_RAW_TOPIC", "pancake.raw")

_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "acks": "all"})


def _delivery_report(err, msg):
    if err is not None:
        logger.error("kafka delivery failed error=%s", err)
    else:
        logger.info(
            "kafka delivered topic=%s partition=%s offset=%s",
            msg.topic(), msg.partition(), msg.offset(),
        )


def _classify_topic(payload: dict[str, Any]) -> str:
    # Chưa có sample payload thật từ Pancake -> đẩy hết vào 1 topic raw để
    # xem dữ liệu thực tế qua Kafka UI rồi mới tách theo type/event.
    return RAW_TOPIC


def _extract_key(payload: dict[str, Any]) -> Optional[str]:
    for field in ("conversation_id", "conversationId", "id"):
        value = payload.get(field)
        if value is not None:
            return str(value)
    return None


def produce_event(payload: dict[str, Any], request_id: str) -> None:
    topic = _classify_topic(payload)
    key = _extract_key(payload)

    logger.info(
        "request_id=%s topic=%s key=%s type=%s",
        request_id, topic, key, payload.get("type") or payload.get("event"),
    )

    _producer.produce(
        topic=topic,
        key=key,
        value=json.dumps(payload),
        headers={"request_id": request_id},
        callback=_delivery_report,
    )
    _producer.poll(0)


def flush(timeout: float = 10) -> None:
    _producer.flush(timeout)
