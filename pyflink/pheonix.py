# pyflink_ais_phonix_job.py
"""
PyFlink streaming job that consumes AIS messages, filters for the vessel
with MMSI 368381530 ("PHONIX") and emits the vessel's positions so they
can be tracked in real‑time.

Assumptions
-----------
* AIS data arrives as **JSON** strings on a Kafka topic called ``ais``.
  Each message looks like::

    {
      "mmsi": 368381530,
      "vessel_name": "PHONIX",
      "lat": 37.7749,
      "lon": -122.4194,
      "sog": 11.2,      # speed over ground (knots)
      "cog": 270.3,      # course over ground (degrees)
      "timestamp": "2025‑05‑27T13:45:05Z"
    }

* We will output the filtered stream **back to Kafka** on a topic called
  ``phonix_track`` in the same JSON format (or you can swap the sink for
  a Print sink while testing).

* Adjust broker addresses, topic names, or the message format to suit
  your environment (search for TODO markers).

This job uses the **DataStream API** because it provides the lowest
latency path and is widely used for shipping to production at Ververica
Platform & Flink SQL Gateway. If you prefer Table API/SQL, the same
logic can be expressed in one INSERT‑SELECT statement – let me know!  

Run locally with the PyFlink Docker image or directly in VVP. Example::

    python pyflink_ais_phonix_job.py \
      --bootstrap.servers localhost:19092 \
      --source-topic ais \
      --sink-topic phonix_track
"""

from __future__ import annotations

import argparse
import json
from typing import Dict, Iterator
from pathlib import Path

from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema, DeliveryGuarantee
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types

# ---------------------------------------------------------------------------
# Command‑line arguments – make it easy to parameterise for dev ↔ prod
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Track the vessel PHONIX with PyFlink")

    parser.add_argument('--bootstrap.servers', default='localhost:19092', dest='brokers',
                        help='Kafka bootstrap servers, comma‑separated')
    parser.add_argument('--source-topic', default='ais', help='Kafka topic that carries raw AIS messages')
    parser.add_argument('--sink-topic', default='phonix_track', help='Kafka topic to publish PHONIX positions to')

    # Flink config shortcuts
    parser.add_argument('--parallelism', type=int, default=1,
                        help='Job parallelism (override for prod)')

    return parser.parse_args()

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def parse_ais_message(json_str: str):
    """Parse AIS JSON message and return dict or None if malformed."""
    try:
        msg = json.loads(json_str)
        return msg
    except Exception as exc:
        print(f"Failed to parse AIS JSON: {exc}")
        return None

def filter_phonix(msg):
    """Filter for PHONIX vessel."""
    if msg is None:
        return False
    
    PHONIX_MMSI = 368381530
    PHONIX_NAME = "PHONIX"
    
    return (msg.get("mmsi") == PHONIX_MMSI or 
            msg.get("vessel_name", "").upper() == PHONIX_NAME)

def to_json_string(msg):
    """Convert message back to JSON string."""
    return json.dumps(msg, separators=(",", ":"))

# ---------------------------------------------------------------------------
# Business logic
# ---------------------------------------------------------------------------

def build_pipeline(env: StreamExecutionEnvironment, args: argparse.Namespace):
    """Define the streaming topology."""

    # === Kafka Source ===
    source: KafkaSource = (
        KafkaSource.builder()
        .set_bootstrap_servers(args.brokers)
        .set_topics(args.source_topic)
        .set_group_id("pyflink‑phonix‑tracker")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # === Kafka Sink ===
    sink: KafkaSink = (
        KafkaSink.builder()
        .set_bootstrap_servers(args.brokers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(args.sink_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    # === DataStream Pipeline ===
    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "ais‑source")
        .map(parse_ais_message, output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(filter_phonix)
        .map(to_json_string, output_type=Types.STRING())
        .sink_to(sink)
        .name("phonix‑to‑kafka")
    )

    # Also print for debugging
    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "ais‑source‑debug")
        .map(parse_ais_message, output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(filter_phonix)
        .print("PHONIX‑pos")
        .name("debug‑print")
    )

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)

    # Add JAR files like in hello_kafka.py and moving_average.py
    env.add_jars(
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/flink-connector-kafka-3.3.0-1.20.jar",
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/kafka-clients-3.6.1.jar"
    )

    # Enable checkpoints every minute for fault‑tolerance (prod default)
    env.enable_checkpointing(60_000)

    build_pipeline(env, args)

    print("Starting PyFlink job: Track PHONIX (MMSI 368381530) ...")
    env.execute("track‑phonix‑vessel")

if __name__ == '__main__':
    main()
