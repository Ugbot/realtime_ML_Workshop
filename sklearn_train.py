import json
import logging
from collections import deque
from typing import List, Tuple, Optional, Deque, Dict, Any

from kafka import KafkaConsumer
import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report

# --- Configuration ---
KAFKA_BROKER: str = "localhost:19092"
KAFKA_SOURCE_TOPIC: str = "coinbase-ticker"
KAFKA_GROUP_ID: str = "sklearn_training_group"
TARGET_PRODUCT_ID: str = "ETH-USD"
FEATURE_WINDOW_SIZE: int = 5  # Number of previous price deltas
PRICE_CHANGE_THRESHOLD: float = 0.0000 # Predict ANY upward movement (adjust if needed)
#PRICE_CHANGE_THRESHOLD: float = 0.0005 # 0.05% price change threshold (like Flink example)
RECORDS_TO_COLLECT: int = 1000 # Number of labeled records for training
MAX_RAW_RECORDS: int = RECORDS_TO_COLLECT + 200 # Collect slightly more raw data
MODEL_FILENAME: str = "eth_price_predictor.joblib"
SCALER_FILENAME: str = "eth_feature_scaler.joblib"

# --- Logger ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger("SklearnTrain")

# --- Helper: Parse Kafka Message ---
def parse_ticker(raw: bytes) -> Optional[Tuple[str, float]]:
    """Extract (product_id, price) from Coinbase JSON."""
    try:
        raw_str = raw.decode('utf-8')
        m = json.loads(raw_str)
        events = m.get("events", [])
        if not events or events[0].get("type") != "update": return None
        tickers = events[0].get("tickers", [])
        if not tickers: return None

        t = tickers[0]
        product_id = t.get("product_id")
        price_str = t.get("price")

        if product_id and price_str:
            return product_id, float(price_str)
        else:
            return None
    except (json.JSONDecodeError, TypeError, KeyError, ValueError, IndexError, UnicodeDecodeError) as exc:
        LOGGER.debug("Dropped message: %s … (%s)", raw[:120], exc)
        return None

# --- Feature and Label Generation ---
def generate_features_and_labels(price_history: List[float]) -> Tuple[List[List[float]], List[int]]:
    """Generate features (deltas) and labels (price up/down) from a list of prices."""
    features: List[List[float]] = []
    labels: List[int] = []
    # Need N prices for features + 1 price for label comparison
    if len(price_history) < FEATURE_WINDOW_SIZE + 1:
        return features, labels

    for i in range(FEATURE_WINDOW_SIZE, len(price_history) - 1):
        # Features: Previous N price deltas
        current_features = [price_history[j] - price_history[j-1] for j in range(i - FEATURE_WINDOW_SIZE + 1, i + 1)]

        # Label: Based on the change from price at index `i` to price at `i+1`
        current_price = price_history[i]
        next_price = price_history[i+1]
        # price_change_ratio = (next_price - current_price) / current_price if current_price != 0 else 0
        # label = 1 if price_change_ratio > PRICE_CHANGE_THRESHOLD else 0 # Use 0 for NOT UP

        # Simpler label: 1 if price increased, 0 otherwise
        label = 1 if next_price > current_price else 0

        features.append(current_features)
        labels.append(label)

    return features, labels

# --- Main Training Logic ---
def main():
    LOGGER.info(f"Connecting to Kafka ({KAFKA_BROKER}) to collect training data...")
    consumer = KafkaConsumer(
        KAFKA_SOURCE_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest', # Start from beginning to get enough data
        consumer_timeout_ms=60000 # Timeout after 60 seconds if no new messages
    )

    raw_prices: List[float] = []
    raw_count = 0
    LOGGER.info(f"Collecting raw {TARGET_PRODUCT_ID} prices (aiming for ~{MAX_RAW_RECORDS})...")

    try:
        for message in consumer:
            raw_count += 1
            parsed = parse_ticker(message.value)
            if parsed and parsed[0] == TARGET_PRODUCT_ID:
                _product_id, price = parsed
                raw_prices.append(price)
                if len(raw_prices) >= MAX_RAW_RECORDS:
                    LOGGER.info(f"Collected {len(raw_prices)} raw prices. Stopping consumer.")
                    break
            if raw_count % 100 == 0:
                 LOGGER.debug(f"Checked {raw_count} raw messages...")

        if len(raw_prices) < FEATURE_WINDOW_SIZE + 1:
            LOGGER.error(f"Insufficient data collected ({len(raw_prices)} prices). Need at least {FEATURE_WINDOW_SIZE + 1}. Exiting.")
            return

    except Exception as e:
        LOGGER.error(f"Error during Kafka consumption: {e}", exc_info=True)
        return
    finally:
        consumer.close()
        LOGGER.info("Kafka consumer closed.")

    LOGGER.info("Generating features and labels...")
    features, labels = generate_features_and_labels(raw_prices)

    if len(features) < RECORDS_TO_COLLECT:
        LOGGER.warning(f"Generated only {len(features)} feature/label pairs (target was {RECORDS_TO_COLLECT}). Training with available data.")
        if not features:
             LOGGER.error("No features could be generated. Exiting.")
             return
    else:
        # Trim to desired number if we overshoot slightly
        features = features[:RECORDS_TO_COLLECT]
        labels = labels[:RECORDS_TO_COLLECT]
        LOGGER.info(f"Using {len(features)} feature/label pairs for training.")


    X = pd.DataFrame(features)
    y = pd.Series(labels)

    LOGGER.info(f"Feature matrix shape: {X.shape}")
    LOGGER.info(f"Label distribution:\n{y.value_counts(normalize=True)}")

    # Split data (optional but good practice)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    # Scale features
    LOGGER.info("Scaling features...")
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test) # Use the same scaler fitted on training data

    # Train model
    LOGGER.info("Training Logistic Regression model...")
    model = LogisticRegression(random_state=42, class_weight='balanced') # Added balancing
    model.fit(X_train_scaled, y_train)

    # Evaluate model
    LOGGER.info("Evaluating model on test set...")
    y_pred = model.predict(X_test_scaled)
    report = classification_report(y_test, y_pred, target_names=['NOT UP', 'UP'])
    LOGGER.info(f"Classification Report:\n{report}")

    # --- Retrain on FULL dataset and Save ---
    LOGGER.info("Retraining model on the full dataset...")
    X_full_scaled = scaler.fit_transform(X) # Fit scaler on ALL data now
    model.fit(X_full_scaled, y) # Retrain on all data

    LOGGER.info(f"Saving scaler to {SCALER_FILENAME}...")
    joblib.dump(scaler, SCALER_FILENAME)
    LOGGER.info(f"Saving model to {MODEL_FILENAME}...")
    joblib.dump(model, MODEL_FILENAME)
    LOGGER.info("Training complete.")

if __name__ == "__main__":
    main() 