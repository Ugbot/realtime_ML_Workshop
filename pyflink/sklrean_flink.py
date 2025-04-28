# pyflink/sklearn_train_flink.py
import json
import logging
from typing import Iterable, Tuple, List, Optional

from pyflink.common import Types, SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.window import GlobalWindow # For typing hint
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

# Imports for sklearn logic (must be installed in Flink's Python environment)
import pandas as pd
import joblib # Although not saving, keep for consistency if extending
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report

# --- Configuration (Mirroring sklearn_train.py) ---
KAFKA_BROKER: str = "localhost:19092"
KAFKA_SOURCE_TOPIC: str = "coinbase-ticker"
# Use a different group ID for this Flink job
KAFKA_GROUP_ID: str = "flink_sklearn_training_group"
TARGET_PRODUCT_ID: str = "ETH-USD"
FEATURE_WINDOW_SIZE: int = 5
RECORDS_TO_COLLECT: int = 1000 # Target number of feature/label pairs
# Window size needs to accommodate feature lookback + collection target
# Collect slightly more raw data to ensure enough feature/label pairs
WINDOW_SIZE: int = RECORDS_TO_COLLECT + FEATURE_WINDOW_SIZE + 50
MODEL_FILENAME: str = "eth_price_predictor.joblib" # For reference
SCALER_FILENAME: str = "eth_feature_scaler.joblib" # For reference

# --- Logger ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger("FlinkSklearnTrain")

# --- Helper: Parse Kafka Message ---
def parse_ticker(raw: str) -> Optional[Tuple[str, float]]:
    """Extract (product_id, price) from Coinbase JSON."""
    try:
        m = json.loads(raw)
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
    except (json.JSONDecodeError, TypeError, KeyError, ValueError, IndexError) as exc:
        LOGGER.debug("Dropped message: %s … (%s)", raw[:120], exc)
        return None

# --- ProcessWindowFunction for Training ---
class TrainEvaluateModelWindow(ProcessWindowFunction):
    """
    Collects prices in a window, generates features/labels,
    trains an sklearn model, evaluates it, and prints the report.
    """
    def process(self,
                key: str,
                context: ProcessWindowFunction.Context, # type: ignore
                elements: Iterable[Tuple[str, float]] # Input: (product_id, price)
               ) -> Iterable[str]:                    # Output: Classification report string

        LOGGER.info(f"Processing window for key '{key}' with {len(list(elements))} elements.")
        # Convert elements back to list for processing
        window_elements = list(elements)
        if len(window_elements) < FEATURE_WINDOW_SIZE + 1:
            LOGGER.warning(f"Window for key '{key}' too small ({len(window_elements)} elements), skipping training.")
            return []

        raw_prices = [price for _pid, price in window_elements]

        # --- Feature and Label Generation (Adapted from sklearn_train.py) ---
        features: List[List[float]] = []
        labels: List[int] = []
        generated_count = 0
        for i in range(FEATURE_WINDOW_SIZE, len(raw_prices) - 1):
            # Features: Previous N price deltas
            current_features = [raw_prices[j] - raw_prices[j-1] for j in range(i - FEATURE_WINDOW_SIZE + 1, i + 1)]
            # Label: 1 if price increased, 0 otherwise
            label = 1 if raw_prices[i+1] > raw_prices[i] else 0
            features.append(current_features)
            labels.append(label)
            generated_count += 1
            if generated_count >= RECORDS_TO_COLLECT: # Stop once target reached
                 break

        if len(features) < 10: # Need a minimum amount of data to train sensibly
            LOGGER.warning(f"Insufficient features generated ({len(features)}) for key '{key}'. Skipping training.")
            return []

        LOGGER.info(f"Generated {len(features)} feature/label pairs for key '{key}'.")
        X = pd.DataFrame(features)
        y = pd.Series(labels)

        label_dist = y.value_counts(normalize=True).to_dict()
        LOGGER.info(f"Label distribution for key '{key}': {label_dist}")

        if len(label_dist) < 2:
             LOGGER.warning(f"Only one class present for key '{key}'. Skipping training.")
             return []

        # --- Model Training & Evaluation (Adapted from sklearn_train.py) ---
        try:
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

            # Scale features
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)

            # Train model
            model = LogisticRegression(random_state=42, class_weight='balanced')
            model.fit(X_train_scaled, y_train)

            # Evaluate model
            y_pred = model.predict(X_test_scaled)
            report = classification_report(y_test, y_pred, target_names=['NOT UP', 'UP'], output_dict=False) # Get string report

            LOGGER.info(f"Training complete for key '{key}'. Evaluation Report:")
            LOGGER.info(f"\n{report}") # Log the report

            # Yield the report string as the result of this function
            yield f"Report for {key}:\n{report}"

        except Exception as e:
            LOGGER.error(f"Error during sklearn training/evaluation for key '{key}': {e}", exc_info=True)
            yield f"Error training model for {key}: {e}" # Output error message


    def clear(self, context: ProcessWindowFunction.Context):
        # No external state to clear in this simple version
        pass

# --- Main Pipeline ---
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/flink-connector-kafka-3.3.0-1.20.jar",
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/kafka-clients-3.6.1.jar",
    )
    # NOTE: sklearn, pandas must be available in the Python execution environment!
    env.set_parallelism(1) # Keep it simple for demo

    # Kafka Source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(KAFKA_SOURCE_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) # Need history
        .set_value_only_deserializer(SimpleStringSchema())
        # Bounded source might be better for pure batch, but let's use window
        # .set_bounded(KafkaOffsetsInitializer.latest())
        .build()
    )

    # --- Flink DataStream Pipeline ---
    (
        env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "KafkaSource")
        .map(parse_ticker, output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE()]))
        .filter(lambda x: x is not None and x[0] == TARGET_PRODUCT_ID)
        .key_by(lambda x: x[0]) # Key by product_id (ETH-USD)
        # Use a CountWindow to collect a batch. Triggers when WINDOW_SIZE elements arrive for the key.
        .count_window(WINDOW_SIZE)
        .process(TrainEvaluateModelWindow(), output_type=Types.STRING())
        .print() # Print the classification report string
    )

    LOGGER.info(f"Starting PyFlink Sklearn Training job (will run until a window of {WINDOW_SIZE} is processed)...")
    env.execute("Flink_Sklearn_Train_Evaluate")

if __name__ == "__main__":
    main()
