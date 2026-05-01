"""
Step 07 – Online Perceptron with keyed state
=============================================
Concepts:  KeyedProcessFunction, ValueStateDescriptor, online ML

This is the first job that carries *mutable state* per key.  Flink manages
the state lifecycle: it is checkpointed, restored on failure, and isolated
between keys.

Pipeline:
  1. Parse ticks → (product_id, price, ts_ms)
  2. Count-window of N+1 ticks → feature vector + label
  3. KeyedProcessFunction updates perceptron weights and predicts

Feature vector: N consecutive price deltas
Label:  +1 if last move > THRESHOLD, else -1

Output (printed):
  (pred=+1, actual=-1, price=3141.59, ts=1712000000000)

Run:
    uv run python 07_online_perceptron.py
"""
import json
import logging
import time
from typing import Iterable, List, Optional, Tuple

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import KeyedProcessFunction, ProcessWindowFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from common import KAFKA_BROKER, SOURCE_TOPIC, TARGET_PRODUCT_ID, build_env

LOG = logging.getLogger("07_perceptron")

FEATURE_WINDOW   = 5      # number of price deltas used as features
WINDOW_SIZE      = FEATURE_WINDOW + 1
THRESHOLD        = 0.0005  # 0.05 % — label +1 if price rises more than this
LEARNING_RATE    = 0.01


# ── 1. Parse (same shape as 06, but we also need ts) ─────────────────────

def parse_with_ts(raw):
    """Returns (product_id, price, ts_ms) or None."""
    try:
        obj = json.loads(raw)
        events = obj.get("events", [])
        if not events:
            return None
        tickers = events[0].get("tickers", [])
        if not tickers:
            return None
        t = tickers[0]
        ts_str = t.get("time", "")
        ts_ms = int(
            time.mktime(time.strptime(ts_str.split(".")[0], "%Y-%m-%dT%H:%M:%S")) * 1000
        ) if ts_str else 0
        return t["product_id"], float(t["price"]), ts_ms
    except Exception:
        return None


# ── 2. Window function: extract features + label ─────────────────────────

class FeatureExtractor(ProcessWindowFunction):
    """
    From WINDOW_SIZE consecutive prices, produce:
      features : FEATURE_WINDOW price deltas
      label    : +1 / -1
      price    : most recent price
      ts_ms    : most recent timestamp
    """

    def process(self, key, ctx, elements):
        # count_window gives no ordering guarantee — sort by ts_ms
        rows = sorted(elements, key=lambda e: e[2])
        prices = [r[1] for r in rows]

        if len(prices) != WINDOW_SIZE:
            return

        deltas = [prices[i] - prices[i - 1] for i in range(1, WINDOW_SIZE)]
        ratio  = (prices[-1] - prices[-2]) / prices[-2] if prices[-2] else 0.0
        label  = 1 if ratio > THRESHOLD else -1

        yield (deltas, label, prices[-1], rows[-1][2])


# ── 3. KeyedProcessFunction: Perceptron ──────────────────────────────────

class OnlinePerceptron(KeyedProcessFunction):
    """
    Maintains weights and bias in Flink ValueState (one set per key).

    State is:
      - checkpointed automatically by Flink
      - restored on job restart
      - logically isolated between keys (different symbols)
    """

    def __init__(self, n_features, lr):
        self.n_features = n_features
        self.lr         = lr
        self._weights   = None   # set in open()
        self._bias      = None

    def open(self, ctx):
        # open() is called once per operator instance before any records arrive.
        # This is where you initialise state descriptors and load external resources.
        self._weights = ctx.get_state(
            ValueStateDescriptor("weights", Types.LIST(Types.DOUBLE()))
        )
        self._bias = ctx.get_state(
            ValueStateDescriptor("bias", Types.DOUBLE())
        )

    def process_element(self, value, ctx):
        features, actual, price, ts_ms = value

        # Read current weights from state (None on first call for this key)
        w = self._weights.value() or [0.0] * self.n_features
        b = self._bias.value()   or 0.0

        # Forward pass
        activation = sum(wi * fi for wi, fi in zip(w, features)) + b
        pred = 1 if activation >= 0.0 else -1

        # Perceptron update rule: only update on mistakes
        if pred != actual:
            w = [wi + self.lr * actual * fi for wi, fi in zip(w, features)]
            b = b + self.lr * actual
            self._weights.update(w)
            self._bias.update(b)

        yield (pred, actual, price, ts_ms)


def main() -> None:
    env = build_env("07_online_perceptron")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("workshop_07")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    tick_type    = Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.LONG()])
    feature_type = Types.TUPLE([
        Types.LIST(Types.DOUBLE()), Types.INT(), Types.DOUBLE(), Types.LONG()
    ])
    pred_type    = Types.TUPLE([Types.INT(), Types.INT(), Types.DOUBLE(), Types.LONG()])

    (
        env
        .from_source(source, WatermarkStrategy.no_watermarks(), "coinbase_src")
        .map(parse_with_ts, output_type=tick_type)
        .filter(lambda x: x is not None and x[0] == TARGET_PRODUCT_ID)
        .key_by(lambda r: r[0])
        .count_window(WINDOW_SIZE, 1)
        .process(FeatureExtractor(), output_type=feature_type)
        # Key by a constant so there is one model for ETH-USD.
        .key_by(lambda _: TARGET_PRODUCT_ID)
        .process(OnlinePerceptron(FEATURE_WINDOW, LEARNING_RATE), output_type=pred_type)
        .print()
    )

    env.execute("07_online_perceptron")


if __name__ == "__main__":
    main()
