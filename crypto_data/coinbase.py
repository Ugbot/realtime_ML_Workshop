#!/usr/bin/env python3
"""
Stream Coinbase Advanced-Trade tickers through an asyncio websocket and
pipe every raw JSON frame straight into the `coinbase-ticker` Kafka topic.
"""

import asyncio
import json
import ssl
from typing import NoReturn

import certifi
import websockets                       # pip install websockets
from confluent_kafka import Producer    # pip install confluent-kafka

WS_URL = "wss://advanced-trade-ws.coinbase.com"
ssl_ctx = ssl.create_default_context(cafile=certifi.where())

# Kafka producer (tune to your cluster)
producer = Producer({"bootstrap.servers": "localhost:19092"})

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
            # Forward raw JSON to Kafka
            producer.produce("coinbase-ticker", value=frame.encode())
            producer.poll(0)  # async flush of background queue
            # Optional: decode for logging / downstream processing
            print("Received:", json.loads(frame))


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
        finally:
            producer.flush()  # ensure all messages are committed


if __name__ == "__main__":
    asyncio.run(main())
