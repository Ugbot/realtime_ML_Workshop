"""
Step 08 – Batch sklearn training inside a Flink count-window
=============================================================
Concepts:  bounded KafkaSource, count_window (tumbling), ProcessWindowFunction,
           sklearn in Flink, model serialisation with joblib

A *bounded* Kafka source reads all existing messages up to the latest offset
at job start-up, then signals completion.  The tumbling count window fires
when WINDOW_SIZE ticks of ETH-USD have been collected.  Because a bounded
source terminates, the job exits naturally after the first (and only) window.

Window elements are sorted by event timestamp before feature construction so
price-delta features are always computed on chronologically ordered prices.

Outputs on stdout:
  - label distribution
  - classification report (held-out 20 %)
  - paths of saved model artefacts

Run:
    uv run python 08_sklearn_train.py
"""
import logging
from pathlib import Path
from typing import List, Optional, Tuple

import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.preprocessing import StandardScaler

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import ProcessWindowFunction
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

from common import (
    KAFKA_BROKER,
    SOURCE_TOPIC,
    TARGET_PRODUCT_ID,
    build_env,
    parse_ticker_ts,
)

LOG = logging.getLogger("08_sklearn_train")

RECORDS_TO_COLLECT  = 1000    # labelled examples to build the model from
FEATURE_WINDOW_SIZE = 5       # N consecutive price deltas per example
# Collect enough raw ticks to produce RECORDS_TO_COLLECT labelled pairs.
# Each labelled example consumes FEATURE_WINDOW_SIZE + 1 prices, but
# consecutive examples share all but one price, so we need:
#   RECORDS_TO_COLLECT + FEATURE_WINDOW_SIZE + 1
# Add a small buffer for any ticks that fail to parse.
WINDOW_SIZE = RECORDS_TO_COLLECT + FEATURE_WINDOW_SIZE + 20

# Label: +1 if price rises by more than THRESHOLD, else 0.
# A small non-zero threshold reduces label noise from bid/ask jitter.
PRICE_CHANGE_THRESHOLD = 0.0002   # 0.02 %

MODEL_PATH  = Path("eth_price_predictor.joblib")
SCALER_PATH = Path("eth_feature_scaler.joblib")


def _label(prev: float, nxt: float) -> int:
    """Return 1 if the price rose by more than PRICE_CHANGE_THRESHOLD, else 0."""
    if prev == 0.0:
        return 0
    return 1 if (nxt - prev) / prev > PRICE_CHANGE_THRESHOLD else 0


class TrainModelWindow(ProcessWindowFunction):
    """
    Tumbling count window that triggers once per WINDOW_SIZE ticks.

    Inside the window:
      1. Sort elements by event timestamp (count windows give no ordering guarantee)
      2. Build feature matrix  X  (price deltas) and label vector  y
      3. Evaluate on a held-out split and report cross-val scores
      4. Retrain on all data and save artefacts
    """

    def process(self, key, ctx, elements):
        # Sort by ts_ms so delta features follow the actual market order.
        rows = sorted(elements, key=lambda r: r[2])
        prices = [r[1] for r in rows]

        LOG.info("Window closed for '%s': %d prices available.", key, len(prices))

        if len(prices) < FEATURE_WINDOW_SIZE + 2:
            LOG.warning("Too few prices (%d) — skipping.", len(prices))
            return

        # ── Feature / label construction ─────────────────────────────────
        features = []   # type: List[List[float]]
        labels   = []   # type: List[int]

        for i in range(FEATURE_WINDOW_SIZE, len(prices) - 1):
            deltas = [
                prices[j] - prices[j - 1]
                for j in range(i - FEATURE_WINDOW_SIZE + 1, i + 1)
            ]
            features.append(deltas)
            labels.append(_label(prices[i], prices[i + 1]))
            if len(features) >= RECORDS_TO_COLLECT:
                break

        if len(set(labels)) < 2:
            LOG.warning("Only one class in training data — cannot train.")
            yield "Error: single class in training data"
            return

        X = pd.DataFrame(features)
        y = pd.Series(labels)

        dist = y.value_counts(normalize=True).to_dict()
        LOG.info("Label distribution: %s", {k: f"{v:.1%}" for k, v in dist.items()})

        # ── Held-out evaluation ──────────────────────────────────────────
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        scaler = StandardScaler()
        X_train_s = scaler.fit_transform(X_train)
        X_test_s  = scaler.transform(X_test)

        model = LogisticRegression(random_state=42, class_weight="balanced", max_iter=500)
        model.fit(X_train_s, y_train)

        report = classification_report(
            y_test,
            model.predict(X_test_s),
            target_names=["FLAT/DOWN", "UP"],
        )

        # 5-fold cross-validation on the full training split (more reliable than
        # a single held-out score on 200 examples).
        cv_scores = cross_val_score(
            LogisticRegression(random_state=42, class_weight="balanced", max_iter=500),
            scaler.fit_transform(X_train),   # refit on full train for CV
            y_train,
            cv=5,
            scoring="f1_macro",
        )
        cv_line = f"5-fold CV F1-macro: {cv_scores.mean():.3f} ± {cv_scores.std():.3f}"

        # ── Retrain on 100 % of the data before saving ───────────────────
        scaler_full = StandardScaler()
        X_full_s = scaler_full.fit_transform(X)
        model_full = LogisticRegression(
            random_state=42, class_weight="balanced", max_iter=500
        )
        model_full.fit(X_full_s, y)

        joblib.dump(scaler_full, str(SCALER_PATH))
        joblib.dump(model_full,  str(MODEL_PATH))
        LOG.info("Saved model → %s   scaler → %s", MODEL_PATH, SCALER_PATH)

        output = (
            f"=== {key} | {len(features)} examples | threshold={PRICE_CHANGE_THRESHOLD} ===\n"
            f"Label distribution: {dist}\n"
            f"{cv_line}\n\n"
            f"Held-out classification report (20 %):\n{report}\n"
            f"Artefacts: {MODEL_PATH}  {SCALER_PATH}"
        )
        yield output


def main() -> None:
    env = build_env("08_sklearn_train")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("workshop_08")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        # Bounded: stop reading when the latest offset at job startup is reached.
        # This makes the source finite so the job exits after the window fires.
        .set_bounded(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # (product_id, price, ts_ms) — timestamp needed for sort inside window
    tick_type = Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.LONG()])

    (
        env
        .from_source(source, WatermarkStrategy.no_watermarks(), "coinbase_src")
        .map(parse_ticker_ts, output_type=tick_type)
        .filter(lambda x: x is not None and x[0] == TARGET_PRODUCT_ID)
        .key_by(lambda r: r[0])
        .count_window(WINDOW_SIZE)
        .process(TrainModelWindow(), output_type=Types.STRING())
        .print()
    )

    LOG.info(
        "Bounded source — reading history.  Need %d ETH-USD ticks to train …",
        WINDOW_SIZE,
    )
    env.execute("08_sklearn_train")


if __name__ == "__main__":
    main()
