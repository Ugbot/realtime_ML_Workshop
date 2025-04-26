# kafka_epoch_converter.py
# Run with:  python kafka_epoch_converter.py

import json
import datetime as dt
from pathlib import Path

from pyflink.common import Types, WatermarkStrategy, SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)

# ---------------------------------------------------------------------
# 1) Helper – convert the Coinbase timestamp to epoch-ms and echo
# ---------------------------------------------------------------------
def to_epoch(json_str: str) -> str:
    try:
        obj = json.loads(json_str)
        if "time" in obj:
            ts = dt.datetime.fromisoformat(obj["time"].replace("Z", "+00:00"))
            obj["time_epoch_ms"] = int(ts.timestamp() * 1000)
        out = json.dumps(obj, separators=(",", ":"))
    except Exception as exc:  # defensive – malformed or missing fields
        out = json.dumps({"parse_error": str(exc), "raw": json_str})
    # print to the same console that started the job
    print(out, flush=True)
    return out


def main() -> None:
    print("Starting Kafka epoch converter …")

    # -----------------------------------------------------------------
    # 2) Execution environment + connector JARs
    # -----------------------------------------------------------------
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    env.add_jars(
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/flink-connector-kafka-3.3.0-1.20.jar",
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/kafka-clients-3.6.1.jar",
    )

    # -----------------------------------------------------------------
    # 3) Kafka source    (from coinbase-ticker)
    # -----------------------------------------------------------------
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:19092")
        .set_topics("coinbase-ticker")
        .set_group_id("pyflink_consumer_group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # -----------------------------------------------------------------
    # 4) Kafka sink      (to coinbase-ticker-epochs)
    # -----------------------------------------------------------------
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("localhost:19092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("coinbase-ticker-epochs")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    # -----------------------------------------------------------------
    # 5) Pipeline
    # -----------------------------------------------------------------
    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "coinbase_source")
        .map(to_epoch, output_type=Types.STRING())
        .sink_to(sink)
    )

    # -----------------------------------------------------------------
    # 6) GO!
    # -----------------------------------------------------------------
    env.execute("Coinbase-timestamp-to-epoch")


if __name__ == "__main__":
    main()
