# kafka_split_by_symbol.py
import json
from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer,
    KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
)
import os

# ──────────────────────────────────────────────────────────
# 1) dynamic topic selector (a plain Python function)
# ──────────────────────────────────────────────────────────
def topic_for(record: str) -> str:
    """
    Return the topic name for one incoming JSON line.
    Falls back to 'unknown-ticker' if the structure is wrong.
    """
    try:
        obj = json.loads(record)
        return obj["events"][0]["tickers"][0]["product_id"]
    except Exception:
        return "unknown-ticker"


def main():
    print("▶ Coinbase fan-out job – one Kafka topic per product_id")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # add the connector + client jars
    env.add_jars(
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/flink-connector-kafka-3.3.0-1.20.jar",
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/kafka-clients-3.6.1.jar",
    )

    # Kafka connection config (Aiven, same as aiven_coinbase.py)
    kafka_bootstrap = "kafka-vvp-aiven-ververica-e42c.c.aivencloud.com:13674"
    kafka_username = "avnadmin"
    kafka_password = "AVNS_v7liC0xcTBV5xVgy6i2"
    ca_path = os.path.join(os.path.dirname(__file__), "../crypto_data/ca.pem")

    # ── Kafka source – the original ticker stream ──
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics("coinbase-ticker")
        .set_group_id("pyflink_splitter")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .set_property("security.protocol", "SASL_SSL")
        .set_property("sasl.mechanism", "PLAIN")
        .set_property("sasl.username", kafka_username)
        .set_property("sasl.password", kafka_password)
        .set_property("ssl.ca.location", ca_path)
        .build()
    )

    # ── Kafka sink – dynamic topic via topic_selector ──
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic_selector(topic_for)              # ★ dynamic routing here
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .set_property("security.protocol", "SASL_SSL")
        .set_property("sasl.mechanism", "PLAIN")
        .set_property("sasl.username", kafka_username)
        .set_property("sasl.password", kafka_password)
        .set_property("ssl.ca.location", ca_path)
        .build()
    )

    # ── Pipe it through unchanged ──
    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "coinbase_src")
        .sink_to(sink)
    )

    env.execute("fan_out_by_symbol")


if __name__ == "__main__":
    main()
