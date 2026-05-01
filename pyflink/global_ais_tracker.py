# pyflink_global_ais_tracker.py
"""
PyFlink streaming job that consumes AIS messages and emits vessel positions
for real-time tracking of all vessels.

Assumptions
-----------
* AIS data arrives as JSON strings on a Kafka topic called ``ais``.
  Each message looks like::

    {
      "mmsi": 368381530,
      "vessel_name": "PHONIX",
      "lat": 37.7749,
      "lon": -122.4194,
      "sog": 11.2,      # speed over ground (knots)
      "cog": 270.3,     # course over ground (degrees)
      "timestamp": "2025‑05‑27T13:45:05Z"
    }

* We will output the filtered stream back to Kafka on a topic called
  ``vessel_tracks`` in the same JSON format.

* Adjust broker addresses, topic names, or the message format to suit
  your environment (search for TODO markers).

This job uses the DataStream API for low latency and production readiness.
"""

from __future__ import annotations

import argparse
import json
from typing import Dict, Optional
from datetime import datetime

from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema, DeliveryGuarantee
)
from pyflink.common.serialization import SimpleStringSchema

# ---------------------------------------------------------------------------
# Command‑line arguments
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Track all vessels with PyFlink")

    parser.add_argument('--bootstrap.servers', default='localhost:19092', dest='brokers',
                        help='Kafka bootstrap servers, comma‑separated')
    parser.add_argument('--source-topic', default='ais', help='Kafka topic that carries raw AIS messages')
    parser.add_argument('--sink-topic', default='vessel_tracks', help='Kafka topic to publish vessel positions to')

    # Flink config shortcuts
    parser.add_argument('--parallelism', type=int, default=1,
                        help='Job parallelism (override for prod)')

    return parser.parse_args()

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def parse_ais_message(json_str: str) -> Optional[Dict]:
    """Parse AIS JSON message and return dict or None if malformed."""
    try:
        msg = json.loads(json_str)
        # Validate required fields
        required_fields = ['mmsi', 'lat', 'lon']
        if not all(field in msg for field in required_fields):
            return None
        return msg
    except Exception as exc:
        print(f"Failed to parse AIS JSON: {exc}")
        return None

def enrich_vessel_data(msg: Dict) -> Dict:
    """Add additional metadata to vessel data."""
    if not msg:
        return msg
    
    # Add received timestamp
    msg['received_at'] = datetime.now().isoformat()
    
    # Add vessel type if not present (could be enriched from a lookup table)
    if 'vessel_type' not in msg:
        msg['vessel_type'] = 'unknown'
    
    return msg

def to_json_string(msg: Dict) -> str:
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
        .set_group_id("pyflink‑global‑vessel‑tracker")
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
        .filter(lambda x: x is not None)  # Remove invalid messages
        .map(enrich_vessel_data, output_type=Types.PICKLED_BYTE_ARRAY())
        .map(to_json_string, output_type=Types.STRING())
        .sink_to(sink)
        .name("vessels‑to‑kafka")
    )

    # Also print for debugging (limit to first 10 vessels to avoid spam)
    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "ais‑source‑debug")
        .map(parse_ais_message, output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda x: x is not None)
        .map(enrich_vessel_data, output_type=Types.PICKLED_BYTE_ARRAY())
        .key_by(lambda x: x['mmsi'])  # Group by vessel
        .map(lambda x: f"Vessel {x['mmsi']} ({x.get('vessel_name', 'unknown')}): {x['lat']}, {x['lon']}")
        .print("vessel‑pos")
        .name("debug‑print")
    )

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)

    # Add JAR files
    env.add_jars(
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/flink-connector-kafka-3.3.0-1.20.jar",
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/kafka-clients-3.6.1.jar"
    )

    # Enable checkpoints every minute for fault‑tolerance
    env.enable_checkpointing(60_000)

    build_pipeline(env, args)

    print("Starting PyFlink job: Track all vessels...")
    env.execute("track‑all‑vessels")

if __name__ == '__main__':
    main() 