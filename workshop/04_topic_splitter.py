"""
Step 04 – Fan-out: one Kafka topic per trading symbol
======================================================
Concepts:  dynamic topic selector, KafkaSink fan-out pattern

The coinbase-ticker topic carries ticks for many symbols (BTC-USD, ETH-USD,
SOL-USD …).  This job routes each message into its own topic so downstream
jobs can subscribe only to the symbols they care about.

Output topics are created automatically by Redpanda on first write.

Run:
    uv run python 04_topic_splitter.py
"""
import json

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

from common import KAFKA_BROKER, SOURCE_TOPIC, build_env


def topic_for(record: str) -> str:
    """
    Route each message to a topic named after its product_id.

    'ETH-USD' message  →  topic 'ETH-USD'
    'BTC-USD' message  →  topic 'BTC-USD'
    Malformed message  →  topic 'unknown-ticker'
    """
    try:
        obj = json.loads(record)
        return obj["events"][0]["tickers"][0]["product_id"]
    except Exception:
        return "unknown-ticker"


def main() -> None:
    env = build_env("04_topic_splitter")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("workshop_04")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            # set_topic_selector replaces set_topic; called once per record.
            .set_topic_selector(topic_for)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    # No transformation needed — just route messages straight through.
    (
        env
        .from_source(source, WatermarkStrategy.no_watermarks(), "coinbase_src")
        .sink_to(sink)
    )

    env.execute("04_topic_splitter")


if __name__ == "__main__":
    main()
