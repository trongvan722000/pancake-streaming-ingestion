import logging
import os
import signal
import time

from clickhouse_driver import Client
from confluent_kafka import Consumer, Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pancake-consumer")

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
RAW_TOPIC = os.environ.get("PANCAKE_RAW_TOPIC", "pancake.raw")
DLQ_TOPIC = os.environ.get("PANCAKE_DLQ_TOPIC", "pancake.raw.dlq")
GROUP_ID = os.environ.get("CONSUMER_GROUP_ID", "pancake-raw-consumer")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
BATCH_FLUSH_SECONDS = float(os.environ.get("BATCH_FLUSH_SECONDS", "5"))


# clickhouse-driver nói native protocol -- port này KHÁC port HTTP (8123),
# không dùng lẫn được (bài học cũ: 2 giao thức, 2 port khác nhau).
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")

_clickhouse = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
)


def _ensure_schema():
    _clickhouse.execute("CREATE DATABASE IF NOT EXISTS dw")
    _clickhouse.execute(
        """
        CREATE TABLE IF NOT EXISTS dw.raw_pancake
        (
            raw_payload      String,
            _kafka_partition Int32,
            _kafka_offset    Int64,
            _ingested_at     DateTime64(3) DEFAULT now64(3)
        )
        ENGINE = MergeTree
        ORDER BY (_ingested_at, _kafka_offset)
        """
    )


_consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    # Tự commit tay sau khi xử lý xong, không để lib commit trước khi chắc chắn đã lưu.
    "enable.auto.commit": False,
})

_dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "acks": "all"})

_running = True


def _handle_shutdown(signum, frame):
    global _running
    logger.info("shutdown signal received, will stop after current batch")
    _running = False


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


def _send_to_dlq(batch, reason):
    for item in batch:
        _dlq_producer.produce(
            topic=DLQ_TOPIC,
            key=item["key"],
            value=item["value"],
            headers={"error_reason": str(reason)[:200]},
        )
    _dlq_producer.flush(10)


def _process_batch(batch):
    rows = [
        (
            item["value"].decode("utf-8", errors="replace") if item["value"] else "",
            item["partition"],
            item["offset"],
        )
        for item in batch
    ]

    # Không tự bắt exception ở đây -- lỗi phải raise lên cho _flush_batch
    # thấy để nó retry/đẩy DLQ đúng như đã thiết kế.
    _clickhouse.execute(
        "INSERT INTO dw.raw_pancake (raw_payload, _kafka_partition, _kafka_offset) VALUES",
        rows,
    )

    logger.info(
        "inserted batch size=%d offset_range=%s..%s",
        len(batch), batch[0]["offset"], batch[-1]["offset"],
    )


def _flush_batch(batch):
    if not batch:
        return

    success = False
    last_error = None
    for attempt in range(1, 4):
        try:
            _process_batch(batch)
            success = True
            break
        except Exception as exc:
            last_error = exc
            logger.warning("process batch failed attempt=%d error=%s", attempt, exc)
            time.sleep(1)

    if success:
        _consumer.commit(asynchronous=False)
        logger.info("batch committed size=%d", len(batch))
    else:
        logger.error("batch failed after retries, sending to DLQ reason=%s", last_error)
        _send_to_dlq(batch, reason=last_error)
        _consumer.commit(asynchronous=False)


def consumer_event():
    _ensure_schema()
    _consumer.subscribe([RAW_TOPIC])
    logger.info("consumer started topic=%s group=%s", RAW_TOPIC, GROUP_ID)

    batch = []
    last_flush = time.time()

    try:
        while _running:
            msg = _consumer.poll(1.0)

            if msg is not None:
                if msg.error():
                    logger.error("consumer error=%s", msg.error())
                else:
                    batch.append({
                        "key": msg.key(),
                        "value": msg.value(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                    })

            batch_full = len(batch) >= BATCH_SIZE
            time_elapsed = (time.time() - last_flush) >= BATCH_FLUSH_SECONDS

            if batch and (batch_full or time_elapsed):
                _flush_batch(batch)
                batch = []
                last_flush = time.time()
    finally:
        logger.info("shutting down, flushing remaining batch size=%d", len(batch))
        _flush_batch(batch)
        _consumer.close()
        logger.info("consumer stopped cleanly")

__all__ = ["consumer_event"]