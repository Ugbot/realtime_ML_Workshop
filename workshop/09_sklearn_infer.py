"""
Step 09 – Live inference with a pre-trained sklearn model
=========================================================
Concepts:  FlatMapFunction, open() lifecycle, PICKLED_BYTE_ARRAY state,
           loading external artefacts into a streaming operator

FlatMapFunction gives you:
  - open()     – called once per operator instance at startup
  - flat_map() – called once per record; may emit 0–N outputs via collector
  - close()    – called when the operator shuts down

Keyed state stores the rolling price history so the feature window
survives Flink checkpoints and restarts without re-warming.

Output (JSON, printed and written to `eth-predictions` Kafka topic):
  {"product_id":"ETH-USD","price":3141.59,"pred":"UP","prob_up":0.713,"prob_flat":0.287}

Pre-requisite: run 08_sklearn_train.py first.

Run:
    uv run python 09_sklearn_infer.py
"""
import json
import logging
from collections import deque
from pathlib import Path
from typing import Deque, List, Optional

import joblib
import pandas as pd

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import FlatMapFunction
from pyflink.datastream.state import ValueStateDescriptor

from common import KAFKA_BROKER, SOURCE_TOPIC, TARGET_PRODUCT_ID, build_env, parse_ticker

LOG = logging.getLogger("09_sklearn_infer")

FEATURE_WINDOW_SIZE = 5       # must match 08_sklearn_train.py
MODEL_PATH  = Path("eth_price_predictor.joblib")
SCALER_PATH = Path("eth_feature_scaler.joblib")


class SklearnPredictor(FlatMapFunction):
    """
    Loads the pre-trained LogisticRegression and StandardScaler on startup,
    maintains a rolling price buffer in keyed state, and emits a JSON
    prediction record for every tick once the buffer has enough data.
    """

    def __init__(self, feature_window, model_path, scaler_path):
        self.feature_window = feature_window
        self.model_path     = str(model_path)
        self.scaler_path    = str(scaler_path)
        self.model          = None
        self.scaler         = None
        self._history_state = None  # initialised in open()

    def open(self, ctx):
        # open() is called once per task slot before any records arrive.
        # Fail fast if artefacts are missing — better than a silent wrong result.
        if not Path(self.model_path).exists():
            raise RuntimeError(
                f"Model not found at {self.model_path}. "
                "Run 08_sklearn_train.py first."
            )
        if not Path(self.scaler_path).exists():
            raise RuntimeError(
                f"Scaler not found at {self.scaler_path}. "
                "Run 08_sklearn_train.py first."
            )
        self.model  = joblib.load(self.model_path)
        self.scaler = joblib.load(self.scaler_path)
        LOG.info("Loaded model (%s) and scaler (%s).", self.model_path, self.scaler_path)

        # PICKLED_BYTE_ARRAY checkpoints arbitrary Python objects (deque here).
        # Keyed state is isolated per key — each symbol gets its own buffer.
        self._history_state = ctx.get_state(
            ValueStateDescriptor("price_history", Types.PICKLED_BYTE_ARRAY())
        )

    def flat_map(self, value, collector):
        _product_id, price = value

        # Restore rolling buffer from Flink state (None on first record for this key).
        history = self._history_state.value()   # type: Optional[Deque[float]]
        if history is None:
            # maxlen ensures the deque automatically discards the oldest price
            # when a new one is appended, keeping memory bounded.
            history = deque(maxlen=self.feature_window + 1)

        history.append(price)
        self._history_state.update(history)

        if len(history) < self.feature_window + 1:
            return  # still warming up — emit nothing

        prices   = list(history)
        # features[i] = prices[i+1] - prices[i], giving feature_window deltas
        features = [prices[i] - prices[i - 1] for i in range(1, self.feature_window + 1)]

        X = self.scaler.transform(pd.DataFrame([features]))
        pred  = int(self.model.predict(X)[0])
        proba = self.model.predict_proba(X)[0]

        collector.collect(json.dumps({
            "product_id": _product_id,
            "price":      round(price, 4),
            "pred":       "UP" if pred == 1 else "FLAT/DOWN",
            "prob_up":    round(float(proba[1]), 4),
            "prob_flat":  round(float(proba[0]), 4),
        }, separators=(",", ":")))


def main() -> None:
    env = build_env("09_sklearn_infer")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("workshop_09")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Sink: predictions land in a dedicated topic for downstream consumption
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("eth-predictions")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    tick_type = Types.TUPLE([Types.STRING(), Types.DOUBLE()])

    predictions = (
        env
        .from_source(source, WatermarkStrategy.no_watermarks(), "coinbase_src")
        .map(parse_ticker, output_type=tick_type)
        .filter(lambda x: x is not None and x[0] == TARGET_PRODUCT_ID)
        .key_by(lambda r: r[0])
        .flat_map(
            SklearnPredictor(FEATURE_WINDOW_SIZE, MODEL_PATH, SCALER_PATH),
            output_type=Types.STRING(),
        )
    )

    # print() for local visibility + sink_to() for downstream composability
    predictions.print()
    predictions.sink_to(sink)

    env.execute("09_sklearn_infer")


if __name__ == "__main__":
    main()
