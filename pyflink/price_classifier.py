#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Online perceptron classifier for ETH-USD Coinbase tick data – PyFlink 1.20
Author: <you>
"""

# ---------------------------------------------------------------------------#
#  Standard library                                                          #
# ---------------------------------------------------------------------------#
import json
import logging
import time
from json import JSONDecodeError
from typing import Iterable, List, Optional, Tuple

# ---------------------------------------------------------------------------#
#  PyFlink 1.20 imports                                                      #
# ---------------------------------------------------------------------------#
from pyflink.common import Types, Time, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import (
    ProcessWindowFunction,
    KeyedProcessFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import CountWindow, TimeWindow
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema

# ---------------------------------------------------------------------------#
#  Configuration                                                             #
# ---------------------------------------------------------------------------#
KAFKA_BROKER = "localhost:19092"
KAFKA_SOURCE_TOPIC = "coinbase-ticker"
KAFKA_GROUP_ID = "pyflink_price_classifier_group"

TARGET_PRODUCT_ID = "ETH-USD"
FEATURE_WINDOW_SIZE = 5  # number of deltas used as features
PRICE_CHANGE_THRESHOLD = 0.0005  # 0.05 %

# connector JARs compiled for Flink-1.20, Scala 2.12
KAFKA_CONNECTOR_JAR = "file:///path/to/flink-connector-kafka-3.3.0-1.20.jar"
KAFKA_CLIENTS_JAR = "file:///path/to/kafka-clients-3.6.1.jar"

# ---------------------------------------------------------------------------#
#  Logger                                                                    #
# ---------------------------------------------------------------------------#
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
LOGGER = logging.getLogger("PriceClassifier")


# ---------------------------------------------------------------------------#
#  Helper – parse Coinbase JSON                                              #
# ---------------------------------------------------------------------------#
def parse_ticker(raw: str) -> Optional[Tuple[str, float, int]]:
    """
    Extract (product_id, price, ts_ms) from a Coinbase message.
    """
    try:
        m = json.loads(raw)
        events = m.get("events", [])
        if not events or events[0].get("type") != "update":
            return None
        tickers = events[0].get("tickers", [])
        if not tickers:
            return None

        t = tickers[0]
        product_id = t.get("product_id")
        price_str = t.get("price")
        ts_str = t.get("time")  # ISO 8601 with optional fractions

        if product_id and price_str and ts_str:
            dt = time.strptime(ts_str.split(".")[0], "%Y-%m-%dT%H:%M:%S")
            ts_ms = int(time.mktime(dt) * 1000)
            return product_id, float(price_str), ts_ms
    except (JSONDecodeError, TypeError, ValueError, KeyError) as exc:
        LOGGER.debug("Dropped message: %.120s (%s)", raw, exc)
    return None


# ---------------------------------------------------------------------------#
#  Watermark strategy                                                        #
# ---------------------------------------------------------------------------#
class PriceTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, _record_ts) -> int:
        # value = (product_id, price, ts_ms)
        return value[2]


watermarks = (
    WatermarkStrategy
        .for_monotonous_timestamps()            # ✅ no out-of-order allowance
        .with_timestamp_assigner(PriceTimestampAssigner())
)


# ---------------------------------------------------------------------------#
#  Window function – build features & label                                  #
# ---------------------------------------------------------------------------#
class ExtractFeaturesAndLabel(ProcessWindowFunction):
    """
    From N+1 consecutive prices produce:

        features : N price deltas
        label    : +1 if next move ↑ > 0.05 %, else −1
        price    : last observed price
        ts_ms    : its timestamp
    """

    def process(
            self,
            key: str,
            ctx: ProcessWindowFunction.Context,  # type: ignore
            elements: Iterable[Tuple[str, float, int]],
    ):
        # ensure chronological order (count window offers no guarantee)
        sorted_elems = sorted(elements, key=lambda e: e[2])
        prices = [e[1] for e in sorted_elems]
        timestamps = [e[2] for e in sorted_elems]

        if len(prices) != FEATURE_WINDOW_SIZE + 1:
            return  # ignore incomplete window

        features = [
            prices[i] - prices[i - 1] for i in range(1, FEATURE_WINDOW_SIZE + 1)
        ]
        prev_p, last_p = prices[-2], prices[-1]
        ratio = (last_p - prev_p) / prev_p if prev_p else 0.0
        label = 1 if ratio > PRICE_CHANGE_THRESHOLD else -1

        yield features, label, last_p, timestamps[-1]

    def clear(self, ctx: ProcessWindowFunction.Context):
        pass  # no per-window state


# ---------------------------------------------------------------------------#
#  Keyed ProcessFunction – online Perceptron                                  #
# ---------------------------------------------------------------------------#
class OnlinePerceptron(KeyedProcessFunction):
    """
    Maintains Perceptron weights & bias in keyed state and updates them online.
    """

    def __init__(self, feature_size: int, lr: float = 0.01):
        self.feature_size = feature_size
        self.lr = lr
        self._weights = None
        self._bias = None

    # -- state init ----------------------------------------------------------
    def open(self, ctx: RuntimeContext):
        self._weights = ctx.get_state(
            ValueStateDescriptor("weights", Types.LIST(Types.DOUBLE()))
        )
        self._bias = ctx.get_state(ValueStateDescriptor("bias", Types.DOUBLE()))

    # -- per-record logic ----------------------------------------------------
    def process_element(self, value, ctx):
        """
        value = (features, actual_label, price, ts_ms)
        """
        features, actual, price, ts_ms = value
        w = self._weights.value() or [0.0] * self.feature_size
        b = self._bias.value() or 0.0

        activation = sum(wi * fi for wi, fi in zip(w, features)) + b
        pred = 1 if activation >= 0 else -1

        # update if mistake
        if pred != actual:
            w = [
                wi + self.lr * actual * fi
                for wi, fi in zip(w, features)
            ]
            b += self.lr * actual
            self._weights.update(w)
            self._bias.update(b)

        yield pred, actual, price, ts_ms


# ---------------------------------------------------------------------------#
#  Main pipeline                                                              #
# ---------------------------------------------------------------------------#
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    env.add_jars(f"file:///Users/bengamble/realtime_ML_Workshop/pyflink/flink-connector-kafka-3.3.0-1.20.jar",
                 "file:///Users/bengamble/realtime_ML_Workshop/pyflink/kafka-clients-3.6.1.jar")  # vital: include file:// prefix

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(KAFKA_SOURCE_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    parsed = (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka")
        .map(parse_ticker, output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.INT()]))
        .filter(lambda x: x and x[0] == TARGET_PRODUCT_ID)
        .assign_timestamps_and_watermarks(watermarks)  # ← new strategy used
    )


    features = (
        parsed
        .key_by(lambda x: x[0], key_type=Types.STRING())  # keyed by product_id
        .count_window(FEATURE_WINDOW_SIZE + 1, 1)
        .process(
            ExtractFeaturesAndLabel(),
            output_type=Types.TUPLE([
                Types.LIST(Types.DOUBLE()), Types.INT(), Types.DOUBLE(), Types.INT()
            ]),
        )
    )

    (
        features
        .key_by(lambda _: TARGET_PRODUCT_ID, key_type=Types.STRING())  # single model
        .process(
            OnlinePerceptron(FEATURE_WINDOW_SIZE),
            output_type=Types.TUPLE([Types.INT(), Types.INT(), Types.DOUBLE(), Types.INT()]),
        )
        .print()
    )

    LOGGER.info("Starting PyFlink 1.20 ETH-USD online classifier …")
    env.execute("ETH-USD Online Perceptron")


# ---------------------------------------------------------------------------#
#  Entry-point                                                                #
# ---------------------------------------------------------------------------#
if __name__ == "__main__":
    main()
