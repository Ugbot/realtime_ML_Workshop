#!/usr/bin/env python3
"""
Stream Coinbase Advanced-Trade tickers through an asyncio websocket and
pipe every raw JSON frame straight into the `coinbase-ticker` Kafka topic.
"""

import asyncio
import json
import ssl
import os
from typing import NoReturn

import certifi
import websockets                       # pip install websockets
from confluent_kafka import Producer    # pip install confluent-kafka

WS_URL = "wss://advanced-trade-ws.coinbase.com"
ssl_ctx = ssl.create_default_context(cafile=certifi.where())

import re
from datetime import datetime, timezone
from typing import Any

# ISO-8601 with optional fractional seconds, always UTC “Z”
_ISO_UTC_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$"
)

def _iso_to_epoch_ms(iso_str: str) -> int:
    """Convert an ISO-8601 UTC string to epoch *milliseconds*."""
    ts = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return int(ts.replace(tzinfo=timezone.utc).timestamp() * 1_000)

def convert_timestamps(obj: Any) -> Any:
    """
    Recursively replace ISO-8601 strings with epoch-ms integers.

    Works for arbitrarily nested dicts / lists.
    """
    if isinstance(obj, dict):
        return {k: convert_timestamps(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_timestamps(v) for v in obj]
    if isinstance(obj, str) and _ISO_UTC_RE.match(obj):
        try:
            return _iso_to_epoch_ms(obj)
        except ValueError:
            # Not a valid ISO timestamp after all – leave untouched
            return obj
    return obj
class KafkaProducerWrapper:
    def __init__(self) -> None:
        ca_path = os.path.join(os.path.dirname(__file__), "ca.pem")
        self.producer = Producer({
            "bootstrap.servers": "kafka-vvp-aiven-ververica-e42c.c.aivencloud.com:13674",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "avnadmin",
            "sasl.password": "AVNS_v7liC0xcTBV5xVgy6i2",
            "security.protocol": "SASL_SSL",
            "ssl.ca.location": ca_path,
        })

    def produce(self, topic: str, value: bytes) -> None:
        self.producer.produce(topic, value=value)
        self.producer.poll(0)

    def flush(self) -> None:
        self.producer.flush()

# Instantiate the producer wrapper
kafka_producer = KafkaProducerWrapper()

# One subscription message covering all desired pairs
SUBSCRIBE_MSG = json.dumps(
    {
        "type": "subscribe",
        "product_ids": [
            "BTC-USD", "ETH-USD", "DOGE-USD", "XRP-USD",
            "LTC-USD", "BCH-USD", "ADA-USD", "SOL-USD",
            "DOT-USD", "LINK-USD", "XLM-USD", "UNI-USD",
            "ALGO-USD", "MATIC-USD",
        ],
        "channel": "ticker",
    }
)


async def stream() -> None:
    """Open the websocket, subscribe, then forward every frame to Kafka."""
    async with websockets.connect(WS_URL, ssl=ssl_ctx, ping_interval=20) as ws:
        await ws.send(SUBSCRIBE_MSG)
        print("Subscribed to ticker channel")

        async for frame in ws:
            raw_data = json.loads(frame)  # decode once
            data = convert_timestamps(raw_data)  # normalise timestamps
            # Forward raw JSON to Kafka
            data = json.dumps(data, separators=(",", ":")).encode()
            kafka_producer.produce("coinbase-ticker",         value=data)

            # Optional: decode for logging / downstream processing
            print("Received:", json.loads(data))


async def main() -> NoReturn:
    """Run `stream()` forever, with automatic back-off reconnects."""
    backoff = 1
    while True:
        try:
            await stream()
        except (websockets.ConnectionClosed, OSError) as exc:
            print(f"Websocket disconnected: {exc!s}. Reconnecting in {backoff}s …")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)  # exponential back-off, capped at 60 s
        else:
            backoff = 1  # reset after a clean run
    # Only flush at shutdown


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        kafka_producer.flush()
