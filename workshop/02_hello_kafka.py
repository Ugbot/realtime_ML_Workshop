"""
Step 02 – Hello Kafka (read and print raw messages)
====================================================
Concepts:  add_jars, KafkaSource, SimpleStringSchema,
           WatermarkStrategy.no_watermarks, from_source

The pipeline reads every message from `coinbase-ticker` and prints it to
stdout.  Press Ctrl-C to stop (unbounded source runs forever).

Pre-requisite: Redpanda running and coinbase-ticker topic populated.

Run:
    uv run python 02_hello_kafka.py
"""
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

from common import KAFKA_BROKER, SOURCE_TOPIC, build_env


def main() -> None:
    env = build_env("02_hello_kafka")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("workshop_02")
        # earliest() replays all stored messages so you see output immediately.
        # Switch to latest() to see only new messages.
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # no_watermarks() is correct here: we are not doing time-based windows.
    (
        env
        .from_source(source, WatermarkStrategy.no_watermarks(), "coinbase_src")
        .print()
    )

    env.execute("02_hello_kafka")


if __name__ == "__main__":
    main()
