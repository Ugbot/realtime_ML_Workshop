# pyflink/sklearn_infer_flink.py
import json
import logging
from collections import deque
from typing import Deque, List, Optional, Tuple

from pyflink.common import Types, SimpleStringSchema, WatermarkStrategy
from pyflink.common.state import ValueState, ValueStateDescriptor
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext, RichFlatMapFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

# Imports for sklearn logic (must be installed in Flink's Python environment)
import pandas as pd
import joblib
from sklearn.preprocessing import StandardScaler # Need this class
from sklearn.linear_model import LogisticRegression # Needed for type hint

# --- Configuration (Mirroring sklearn_infer.py) ---
KAFKA_BROKER: str = "localhost:19092"
KAFKA_SOURCE_TOPIC: str = "coinbase-ticker"
KAFKA_GROUP_ID: str = "flink_sklearn_inference_group" # Different group ID
TARGET_PRODUCT_ID: str = "ETH-USD"
FEATURE_WINDOW_SIZE: int = 5  # MUST match the training script
# !! Model and Scaler files must be accessible by Flink TaskManagers !!
MODEL_FILENAME: str = "eth_price_predictor.joblib"
SCALER_FILENAME: str = "eth_feature_scaler.joblib"

# --- Logger ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger("FlinkSklearnInfer")

# --- Helper: Parse Kafka Message (same as training) ---
def parse_ticker(raw: str) -> Optional[Tuple[str, float]]:
    """Extract (product_id, price) from Coinbase JSON."""
    # Reusing the parser, assumes input is string from SimpleStringSchema
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

# --- RichFlatMapFunction for Inference ---
class SklearnPredictor(RichFlatMapFunction):
    """
    Loads a pre-trained sklearn model and scaler, maintains price history
    in state, generates features, and predicts price movement.
    """
    def __init__(self, model_path: str, scaler_path: str, feature_window: int):
        self.model_path = model_path
        self.scaler_path = scaler_path
        self.feature_window = feature_window
        self.model: Optional[LogisticRegression] = None
        self.scaler: Optional[StandardScaler] = None
        # State to hold the recent price history (using ValueState with a deque)
        self.price_history_state: Optional[ValueState[Deque[float]]] = None

    def open(self, runtime_context: RuntimeContext):
        LOGGER.info(f"Loading model from {self.model_path} and scaler from {self.scaler_path}")
        try:
            # !! Files must be accessible from where this runs !!
            self.model = joblib.load(self.model_path)
            self.scaler = joblib.load(self.scaler_path)
            LOGGER.info("Model and scaler loaded successfully.")
        except FileNotFoundError:
            LOGGER.error(f"CRITICAL: Model or Scaler file not found at specified paths.")
            LOGGER.error(f"Model path: {self.model_path}, Scaler path: {self.scaler_path}")
            # Raising an exception might be better to halt the job if model is essential
            raise RuntimeError("Could not load model/scaler files.")
        except Exception as e:
            LOGGER.error(f"Error loading model or scaler: {e}", exc_info=True)
            raise RuntimeError(f"Failed to load model/scaler: {e}")

        # Initialize state for price history
        # Store deque in ValueState (requires pickling)
        state_desc = ValueStateDescriptor(
            "price_history",
            Types.PICKLED_BYTE_ARRAY() # Use pickle for deque
        )
        self.price_history_state = runtime_context.get_state(state_desc)
        LOGGER.info("Price history state initialized.")

    def flat_map(self,
                 value: Tuple[str, float], # Input: (product_id, price)
                 collector # type: ignore
                 ) -> None:
        _product_id, price = value

        # Get current price history from state
        history: Deque[float] = self.price_history_state.value() # type: ignore
        if history is None:
            # Initialize deque with fixed size N+1 for N features
            history = deque(maxlen=self.feature_window + 1)

        # Add current price
        history.append(price)

        # Update state
        self.price_history_state.update(history) # type: ignore

        # Check if we have enough data for features
        if len(history) < self.feature_window + 1:
            # Not enough history yet
            return

        # --- Generate Features (same logic as sklearn_infer.py) ---
        prices = list(history)
        # Features: N most recent price differences
        features = [prices[i] - prices[i-1] for i in range(1, self.feature_window + 1)] # Indexing from 1 for N deltas

        # --- Predict ---
        try:
            # Reshape features for scaler/model (expects 2D array)
            features_df = pd.DataFrame([features])
            # Scale features
            features_scaled = self.scaler.transform(features_df) # type: ignore
            # Make prediction
            prediction = self.model.predict(features_scaled)[0] # type: ignore
            proba = self.model.predict_proba(features_scaled)[0] # type: ignore

            prediction_label = "UP" if prediction == 1 else "NOT UP"
            result = (
                f"Price: {price:.2f} -> Prediction: {prediction_label} "
                f"(Prob UP: {proba[1]:.3f}, Prob NOT UP: {proba[0]:.3f})"
            )
            # Output the prediction string
            collector.collect(result)

        except Exception as e:
            LOGGER.error(f"Error during prediction for price {price}: {e}", exc_info=False) # Avoid flooding logs
            collector.collect(f"Error predicting for price {price}")


# --- Main Pipeline ---
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/flink-connector-kafka-3.3.0-1.20.jar",
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/kafka-clients-3.6.1.jar",
    )
    # NOTE: sklearn, pandas, joblib must be available in the Python execution environment!
    env.set_parallelism(1) # Keep it simple

    # Kafka Source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(KAFKA_SOURCE_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) # Start from new data
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # --- Flink DataStream Pipeline ---
    (
        env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "KafkaSource")
        .map(parse_ticker, output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE()]))
        .filter(lambda x: x is not None and x[0] == TARGET_PRODUCT_ID)
        .key_by(lambda x: x[0]) # Key by product_id (ETH-USD)
        # Apply the RichFlatMapFunction for prediction
        .flat_map(SklearnPredictor(MODEL_FILENAME, SCALER_FILENAME, FEATURE_WINDOW_SIZE),
                  output_type=Types.STRING())
        .print() # Print the prediction result string
    )

    LOGGER.info("Starting PyFlink Sklearn Inference job...")
    env.execute("Flink_Sklearn_Inference")

if __name__ == "__main__":
    main()