# kafka_print_datastream_local.py
#
# Requirements ──────────────────────────────────────────────────────────────
#   - PyFlink installed (matching your Flink distribution)
#   - Kafka running on localhost:19091
#   - The Kafka connector JAR (e.g. flink-connector-kafka-3.3.0-1.20.jar)
#     placed in the same folder as this script.
#
# Usage:
#   $ python kafka_print_datastream_local.py

from pathlib import Path

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource


def main() -> None:
    # ── 1. Locate the connector JAR sitting next to the script ────────────
    # Adjust the file name below if you use a different connector version.
    # jar_file = Path(__file__).with_name(
    #     "/Users/bengamble/realtime_ML_Workshop/pyflink/flink-connector-kafka-3.3.0-1.20.jar"  # ← change if needed
    # ).resolve()
    #
    # if not jar_file.exists():
    #     raise FileNotFoundError(
    #         f"Kafka connector JAR not found at {jar_file}. "
    #         "Place the JAR next to this script or fix the file name."
    #     )

    # ── 2. Execution environment ───────────────────────────────────────────
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(f"file:///Users/bengamble/realtime_ML_Workshop/pyflink/flink-connector-kafka-3.3.0-1.20.jar",
                 "file:///Users/bengamble/realtime_ML_Workshop/pyflink/kafka-clients-3.6.1.jar")  # vital: include file:// prefix

    # ── 3. Define the Kafka source ─────────────────────────────────────────
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:19092")
        .set_topics("coinbase-ticker")                       # change topic if needed
        .set_group_id("pyflink_consumer_group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # ── 4. Build and execute the pipeline ─────────────────────────────────
    env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "local_kafka_source",
    ).print()

    env.execute("kafka_print_datastream_local")


if __name__ == "__main__":
    main()
