"""
common.py – shared utilities for every workshop script.

Provides:
  - JAR paths resolved relative to the repo layout
  - Kafka broker address
  - parse_ticker()    – decode a Coinbase message → (product_id, price)
  - parse_ticker_ts() – same, also returns ts_ms for ordered processing
  - build_env()       – create a StreamExecutionEnvironment with JARs attached
"""
import datetime
import json
import logging
from pathlib import Path
from typing import Optional, Tuple

from pyflink.datastream import StreamExecutionEnvironment

# ── JAR locations ─────────────────────────────────────────────────────────
# JARs live in  <repo-root>/pyflink/
# This file lives in  <repo-root>/workshop/
_JAR_DIR = Path(__file__).parent.parent / "pyflink"

CONNECTOR_JAR = f"file://{_JAR_DIR / 'flink-connector-kafka-3.3.0-1.20.jar'}"
CLIENTS_JAR   = f"file://{_JAR_DIR / 'kafka-clients-3.6.1.jar'}"

# ── Kafka ─────────────────────────────────────────────────────────────────
KAFKA_BROKER        = "localhost:19092"
SOURCE_TOPIC        = "coinbase-ticker"
TARGET_PRODUCT_ID   = "ETH-USD"

# ── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
)


def build_env(job_name: str = "workshop", parallelism: int = 1) -> StreamExecutionEnvironment:
    """Return an environment with Kafka JARs already registered."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)
    env.add_jars(CONNECTOR_JAR, CLIENTS_JAR)
    return env


def parse_ticker(raw: str) -> Optional[Tuple[str, float]]:
    """
    Decode one Coinbase WebSocket JSON message.

    Returns (product_id, price) on success, None on any parse failure.

    Expected shape::

        {
          "events": [
            {
              "type": "update",
              "tickers": [
                {"product_id": "ETH-USD", "price": "3141.59", ...}
              ]
            }
          ]
        }
    """
    log = logging.getLogger("parse_ticker")
    try:
        m = json.loads(raw)
        events = m.get("events", [])
        if not events:
            return None
        tickers = events[0].get("tickers", [])
        if not tickers:
            return None
        t = tickers[0]
        return t["product_id"], float(t["price"])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        log.debug("Dropped: %.120s  (%s)", raw, exc)
        return None


def parse_ticker_ts(raw: str) -> Optional[Tuple[str, float, int]]:
    """
    Decode one Coinbase WebSocket JSON message including the event timestamp.

    Returns (product_id, price, ts_ms) on success, None on any parse failure.
    ts_ms is milliseconds since the Unix epoch, taken from the ticker's `time`
    field (ISO-8601).  Used by scripts that need to sort window contents by
    event time.
    """
    log = logging.getLogger("parse_ticker_ts")
    try:
        m = json.loads(raw)
        events = m.get("events", [])
        if not events:
            return None
        tickers = events[0].get("tickers", [])
        if not tickers:
            return None
        t = tickers[0]
        ts_str = t.get("time", "")
        if ts_str:
            ts_ms = int(
                datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                .timestamp() * 1000
            )
        else:
            # Fall back to the top-level timestamp field if present
            top_ts = m.get("timestamp", "")
            ts_ms = int(
                datetime.datetime.fromisoformat(top_ts.replace("Z", "+00:00"))
                .timestamp() * 1000
            ) if top_ts else 0
        return t["product_id"], float(t["price"]), ts_ms
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        log.debug("Dropped: %.120s  (%s)", raw, exc)
        return None
