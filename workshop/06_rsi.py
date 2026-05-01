"""
Step 06 – 14-period RSI via sliding count window
=================================================
Concepts:  count_window (sliding), ProcessWindowFunction, keyed state

RSI (Relative Strength Index) is a momentum indicator.  It requires N+1
prices to compute N deltas, so we collect 101 ticks (100 deltas) in a
count-based sliding window that advances one tick at a time.

Output topic: eth-rsi
  {"product_id":"ETH-USD","price":3141.59,"rsi14":62.3401}

Run:
    uv run python 06_rsi.py
"""
import json
import logging
from typing import Iterable, List, Tuple

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import ProcessWindowFunction
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

from common import KAFKA_BROKER, SOURCE_TOPIC, TARGET_PRODUCT_ID, build_env, parse_ticker

LOG = logging.getLogger("06_rsi")

# Number of deltas used for RSI; standard is 14.
# We need RSI_PERIOD + 1 prices → RSI_PERIOD + 1 ticks in the window.
RSI_PERIOD = 14
WINDOW_SIZE = RSI_PERIOD + 1   # 15 ticks ⇒ 14 deltas, slide by 1


class RSIWindow(ProcessWindowFunction):
    """
    ProcessWindowFunction has access to the window context (timers, metadata).
    For a count window it gives us nothing extra over WindowFunction, but it's
    the right choice when you may want to add side-outputs or late-data handling
    later.
    """

    def process(
        self,
        key,               # the grouped key (product_id string)
        ctx,               # ProcessWindowFunction.Context
        elements,          # Iterable[Tuple[str, float]]
    ):
        closes = [r[1] for r in elements]

        if len(closes) < WINDOW_SIZE:
            # Window not full yet (shouldn't happen with count_window, but be safe)
            return

        gains  = [max(0.0, closes[i+1] - closes[i]) for i in range(RSI_PERIOD)]
        losses = [max(0.0, closes[i] - closes[i+1]) for i in range(RSI_PERIOD)]

        avg_gain = sum(gains)  / RSI_PERIOD
        avg_loss = sum(losses) / RSI_PERIOD

        # Standard Wilder RSI formula
        rsi = 100.0 if avg_loss == 0.0 else 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)

        yield json.dumps(
            {"product_id": key, "price": closes[-1], "rsi14": round(rsi, 4)},
            separators=(",", ":"),
        )


def main() -> None:
    env = build_env("06_rsi")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("workshop_06")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("eth-rsi")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    tick_type = Types.TUPLE([Types.STRING(), Types.DOUBLE()])

    (
        env
        .from_source(source, WatermarkStrategy.no_watermarks(), "coinbase_src")
        .map(parse_ticker, output_type=tick_type)
        .filter(lambda x: x is not None and x[0] == TARGET_PRODUCT_ID)
        .key_by(lambda r: r[0])
        # count_window(size, slide): triggers every `slide` records,
        # retaining the last `size` records.
        .count_window(WINDOW_SIZE, 1)
        .process(RSIWindow(), output_type=Types.STRING())
        .sink_to(sink)
    )

    env.execute("06_rsi")


if __name__ == "__main__":
    main()
