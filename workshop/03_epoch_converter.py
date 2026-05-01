"""
Step 03 – Stateless map transform with a Kafka sink
====================================================
Concepts:  map with output_type, KafkaSink, KafkaRecordSerializationSchema,
           DeliveryGuarantee

Reads from `coinbase-ticker`, adds a `time_epoch_ms` field to each JSON
message, then writes the enriched messages to `coinbase-ticker-epochs`.

Run:
    uv run python 03_epoch_converter.py
"""
import datetime
import json

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

from common import KAFKA_BROKER, SOURCE_TOPIC, build_env


def add_epoch(raw: str) -> str:
    """
    Add `time_epoch_ms` (int) to the top-level JSON object.

    Coinbase messages carry an ISO-8601 `time` field at the top level.
    Converting to epoch-ms lets downstream jobs assign event-time watermarks
    without re-parsing ISO strings.
    """
    try:
        obj = json.loads(raw)
        ts_str = obj.get("time", "")
        if ts_str:
            ts = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            obj["time_epoch_ms"] = int(ts.timestamp() * 1000)
        return json.dumps(obj, separators=(",", ":"))
    except Exception as exc:
        # Preserve the original message on parse error so nothing is silently lost.
        return json.dumps({"parse_error": str(exc), "raw": raw}, separators=(",", ":"))


def main() -> None:
    env = build_env("03_epoch_converter")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("workshop_03")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("coinbase-ticker-epochs")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        # AT_LEAST_ONCE is fine for analytics; use EXACTLY_ONCE for financial data
        # with a checkpoint interval configured on the environment.
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    (
        env
        .from_source(source, WatermarkStrategy.no_watermarks(), "coinbase_src")
        .map(add_epoch, output_type=Types.STRING())
        .sink_to(sink)
    )

    env.execute("03_epoch_converter")


if __name__ == "__main__":
    main()
