"""
Step 05 – 5-second sliding moving average (event-time windows)
==============================================================
Concepts:  assign_timestamps_and_watermarks, TimestampAssigner,
           key_by, SlidingEventTimeWindows, WindowFunction

Event-time processing requires three things:
  1. A timestamp extractor  – tells Flink which field is "the clock"
  2. A watermark generator  – declares how late records can arrive
  3. A window definition    – declares the size and slide of the window

Output is written to per-symbol topics  '<SYMBOL>-ma5s'
(e.g. ETH-USD-ma5s, BTC-USD-ma5s).

Run:
    uv run python 05_moving_average.py
"""
import json
from typing import Iterable

from pyflink.common import Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import WindowFunction
from pyflink.datastream.window import SlidingEventTimeWindows

from common import KAFKA_BROKER, SOURCE_TOPIC, build_env, parse_ticker


# ── Step 1: extract the event timestamp from each record ──────────────────
# Our parsed record is (product_id: str, price: float, ts_ms: int).
# The TimestampAssigner tells Flink which element field is the event clock.

class TickTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, _record_ts):
        # value[2] is ts_ms (epoch milliseconds)
        return value[2]


# ── Parse enriched with timestamp ─────────────────────────────────────────

def parse_with_ts(raw):
    """
    Returns (product_id, price, ts_ms) or None.

    Extends parse_ticker() to also extract the ISO-8601 timestamp from the
    top-level 'timestamp' field and convert it to epoch-ms.
    """
    import datetime
    try:
        obj = json.loads(raw)
        ts_str = obj.get("timestamp", "")
        ts_ms = int(
            datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            .timestamp() * 1000
        )
        events = obj.get("events", [])
        if not events:
            return None
        tickers = events[0].get("tickers", [])
        if not tickers:
            return None
        t = tickers[0]
        return t["product_id"], float(t["price"]), ts_ms
    except Exception:
        return None


# ── Step 2: the window function ───────────────────────────────────────────

class MovingAverageWindow(WindowFunction):
    """
    WindowFunction receives all records in a completed window at once.
    Simpler than ProcessWindowFunction – no access to timers or side outputs.
    """

    def apply(self, key, window, inputs, out):
        # inputs is an Iterable of (product_id, price, ts_ms)
        prices = [rec[1] for rec in inputs]
        if not prices:
            return
        avg = sum(prices) / len(prices)
        result = json.dumps(
            {
                "product_id": key,
                "window_end_ms": window.get_end(),
                "ma5s": round(avg, 6),
                "n": len(prices),
            },
            separators=(",", ":"),
        )
        out.collect(result)


# ── Step 3: dynamic sink topic ────────────────────────────────────────────

def ma_topic(record):
    try:
        return json.loads(record)["product_id"] + "-ma5s"
    except Exception:
        return "unknown-ma5s"


def main() -> None:
    env = build_env("05_moving_average")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("workshop_05")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic_selector(ma_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    record_type = Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.LONG()])

    (
        env
        .from_source(source, WatermarkStrategy.no_watermarks(), "coinbase_src")
        # Parse to (product_id, price, ts_ms) — drop malformed messages
        .map(parse_with_ts, output_type=record_type)
        .filter(lambda x: x is not None)
        # Attach event-time and watermarks AFTER parsing so we have ts_ms
        .assign_timestamps_and_watermarks(
            WatermarkStrategy
            .for_bounded_out_of_orderness(Time.seconds(1))
            .with_timestamp_assigner(TickTimestampAssigner())
        )
        .key_by(lambda r: r[0])   # group per symbol
        # 5-second window, slides every 1 second
        .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
        .apply(MovingAverageWindow(), output_type=Types.STRING())
        .sink_to(sink)
    )

    env.execute("05_moving_average")


if __name__ == "__main__":
    main()
