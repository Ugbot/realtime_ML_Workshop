# eth_rsi_simple.py  –  PyFlink 1.20  (≈ 70 LOC)

import json
import logging
from json import JSONDecodeError
from typing import Iterable, Tuple, List, Optional

from pyflink.common import Types, SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.window import CountWindow            # only for hints
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema, DeliveryGuarantee
)

RSI_WINDOW = 100

# logger (put this once near the top of the file)
LOGGER = logging.getLogger("eth_rsi")          # keep INFO for useful output
LOGGER.setLevel(logging.INFO)


# ───────────────────────── helper to parse the incoming JSON ──────────────────
def parse_ticker(raw: str) -> Optional[Tuple[str, float]]:
    """
    Extract (product_id, price) from a Coinbase websocket payload.

    Returns
    -------
    tuple | None
        • (product_id, price) on success
        • None when the JSON is malformed, not the expected "ticker",
          or any required field is missing.
    """
    try:
        m = json.loads(raw)
        # guard against missing keys or wrong event type
        events = m.get("events", [])
        if not events:
            raise KeyError("events[] empty")

        tickers = events[0].get("tickers", [])
        if not tickers:
            raise KeyError("tickers[] empty")

        t = tickers[0]
        return t["product_id"], float(t["price"])

    except (JSONDecodeError, TypeError, KeyError, ValueError) as exc:
        # DEBUG for volume, INFO only if you need to investigate bad data
        LOGGER.debug("Dropped message: %s … (%s)", raw[:120], exc)
        return "fail", 0.0

# ───────────── window function: 14-period RSI on 15-wide sliding window ────────
class RSI14(ProcessWindowFunction):

    def process(self,
                key: str,
                ctx,                                 # ProcessWindowFunction.Context
                elements: Iterable[Tuple[str, float]]
               ) -> Iterable[str]:                   #  ← return an iterable
        closes = [r[1] for r in elements]
        if len(closes) <= RSI_WINDOW :                        # 15 ticks ⇒ 14 deltas
            return []                               # emit nothing yet

        gains  = [max(0, closes[i+1] - closes[i]) for i in range(RSI_WINDOW-1)]
        losses = [max(0, closes[i] - closes[i+1]) for i in range(RSI_WINDOW-1)]
        avg_gain, avg_loss = sum(gains)/RSI_WINDOW, sum(losses)/RSI_WINDOW
        rsi = 100.0 if avg_loss == 0 else 100 - 100/(1 + avg_gain/avg_loss)

        return [json.dumps({
            "product_id": key,
            "price": closes[-1],
            "rsi14": round(rsi, 4)
        }, separators=(",", ":"))]


# ──────────────────────────────── pipeline ────────────────────────────────────
def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/flink-connector-kafka-3.3.0-1.20.jar",
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/kafka-clients-3.6.1.jar",
    )
    # single-task demo

    # Kafka source
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:19092")
        .set_topics("coinbase-ticker")
        .set_group_id("pyflink_eth_rsi")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Kafka sink
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("localhost:19092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("eth-rsi")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "src")  # :contentReference[oaicite:2]{index=2}
          .map(parse_ticker,
               output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE()]))
          .filter(lambda x: x is not None and x[0] == "ETH-USD")
          .key_by(lambda r: r[0])
          .count_window(RSI_WINDOW+1, 1)                          # width 15 / slide 1 :contentReference[oaicite:3]{index=3}
          .process(RSI14(), output_type=Types.STRING())
          .print()                                      # quick local view :contentReference[oaicite:4]{index=4}
          # .sink_to(sink)
    )

    env.execute("eth_usd_rsi14_simple")

if __name__ == "__main__":
    main()
