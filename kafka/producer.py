"""
Kafka Producer — Sends events to a Kafka topic.
"""
import json
import os
from datetime import datetime

from confluent_kafka import Producer
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_RAW", "raw-events")


def delivery_report(err, msg):
    if err:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def create_producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


def send_event(producer: Producer, event: dict) -> None:
    payload = json.dumps({**event, "timestamp": datetime.utcnow().isoformat()})
    producer.produce(TOPIC, value=payload.encode("utf-8"), callback=delivery_report)
    producer.poll(0)


def flush(producer: Producer) -> None:
    producer.flush()
    logger.info("All messages flushed.")


if __name__ == "__main__":
    p = create_producer()
    sample_events = [
        {"user_id": "u001", "event": "page_view", "page": "/home"},
        {"user_id": "u002", "event": "purchase", "amount": 150.0},
        {"user_id": "u003", "event": "signup", "source": "organic"},
    ]
    for event in sample_events:
        send_event(p, event)
    flush(p)
