import json
import logging
from collections import deque
from typing import List, Tuple, Optional

from kafka import KafkaConsumer
import pandas as pd
import joblib
from sklearn.preprocessing import StandardScaler # Need this class
from sklearn.linear_model import LogisticRegression # Only needed if type hinting model explicitly

# --- Configuration ---
KAFKA_BROKER: str = "localhost:19092"
KAFKA_SOURCE_TOPIC: str = "coinbase-ticker"
# Use a different group_id for inference to avoid conflicts if run concurrently
KAFKA_GROUP_ID: str = "sklearn_inference_group"
TARGET_PRODUCT_ID: str = "ETH-USD"
FEATURE_WINDOW_SIZE: int = 5  # MUST match the training script
MODEL_FILENAME: str = "eth_price_predictor.joblib"
SCALER_FILENAME: str = "eth_feature_scaler.joblib"

# --- Logger ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger("SklearnInference")

# --- Helper: Parse Kafka Message (same as training script) ---
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

# --- Feature Generation (only needs N features for prediction) ---
def generate_features(price_history: deque) -> Optional[List[float]]:
    """Generate features (deltas) from price history for prediction."""
    if len(price_history) < FEATURE_WINDOW_SIZE + 1:
         # Need N+1 prices to calculate N deltas
        return None

    prices = list(price_history)
     # Features: N most recent price differences (same as training)
    features = [prices[i] - prices[i-1] for i in range(-FEATURE_WINDOW_SIZE, 0)]
    return features

# --- Main Inference Logic ---
def main():
    LOGGER.info("Loading model and scaler...")
    try:
        model: LogisticRegression = joblib.load(MODEL_FILENAME) # Added type hint
        scaler: StandardScaler = joblib.load(SCALER_FILENAME) # Added type hint
        LOGGER.info(f"Model loaded from {MODEL_FILENAME}")
        LOGGER.info(f"Scaler loaded from {SCALER_FILENAME}")
    except FileNotFoundError:
        LOGGER.error(f"Error: Model ('{MODEL_FILENAME}') or Scaler ('{SCALER_FILENAME}') file not found.")
        LOGGER.error("Please run the training script (sklearn_train.py) first.")
        return
    except Exception as e:
        LOGGER.error(f"Error loading model or scaler: {e}")
        return

    LOGGER.info(f"Connecting to Kafka ({KAFKA_BROKER}) for live inference on {TARGET_PRODUCT_ID}...")
    consumer = KafkaConsumer(
        KAFKA_SOURCE_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='latest', # Start from new messages for inference
        consumer_timeout_ms= -1 # Run indefinitely until stopped
    )

    # Buffer needs N+1 points to calculate N features
    price_buffer = deque(maxlen=FEATURE_WINDOW_SIZE + 1)
    message_count = 0

    LOGGER.info("Starting inference loop (Press Ctrl+C to stop)...")
    try:
        for message in consumer:
            message_count += 1
            parsed = parse_ticker(message.value)
            if parsed and parsed[0] == TARGET_PRODUCT_ID:
                _product_id, price = parsed
                price_buffer.append(price)

                # Generate features if buffer is full enough
                features = generate_features(price_buffer)

                if features:
                    # Reshape features for scaler/model (expects 2D array)
                    features_df = pd.DataFrame([features])
                    # Scale features using the loaded scaler
                    features_scaled = scaler.transform(features_df)
                    # Make prediction
                    prediction = model.predict(features_scaled)[0] # Get the single prediction
                    proba = model.predict_proba(features_scaled)[0] # Get probabilities

                    prediction_label = "UP" if prediction == 1 else "NOT UP"
                    LOGGER.info(f"Price: {price:.2f} -> Prediction: {prediction_label} (Prob UP: {proba[1]:.3f}, Prob NOT UP: {proba[0]:.3f})")

            if message_count % 100 == 0:
                LOGGER.debug(f"Processed {message_count} messages...")

    except KeyboardInterrupt:
        LOGGER.info("Interrupted by user. Stopping inference.")
    except Exception as e:
        LOGGER.error(f"Error during Kafka consumption/inference: {e}", exc_info=True)
    finally:
        consumer.close()
        LOGGER.info("Kafka consumer closed.")


if __name__ == "__main__":
    main() 