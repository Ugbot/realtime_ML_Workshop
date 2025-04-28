# kafka_ma5s.py  – 5-second moving average per product_id
import json, datetime as dt
from pyflink.common import (Time, SimpleStringSchema, WatermarkStrategy,
                            Types)
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import WindowFunction
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema, DeliveryGuarantee
)

# ────────────────────── helpers ──────────────────────
def parse(js: str):
    """→ (product_id, price, epoch_ms)  or  None if malformed."""
    try:
        obj = json.loads(js)
        ticker = obj["events"][0]["tickers"][0]
        ts_iso = obj["timestamp"]
        ts_ms = int(dt.datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
                          .timestamp() * 1000)
        return ticker["product_id"], float(ticker["price"]), ts_ms
    except Exception:
        return None


class MA5Window(WindowFunction):
    """emit averaged price for each 5-sec window"""
    def apply(self, key, window, inputs, out):
        prices = [rec[1] for rec in inputs]
        avg = sum(prices) / len(prices)
        result = json.dumps(
            {"product_id": key,
             "window_end": window.get_end(),   # epoch-ms
             "ma5s": round(avg, 6)},
            separators=(",", ":")
        )
        print(result, flush=True)
        out.collect(result)


def topic_selector(record: str) -> str:
    """Route MA output to '<symbol>-ma5s'."""
    try:
        prod = json.loads(record)["product_id"]
        return f"{prod}-ma5s"
    except Exception:
        return "unknown-ma5s"

# ──────────────────── job definition ────────────────────
def main():
    print("▶ 5-second moving-average job starting …")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # connector jars
    env.add_jars(
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/flink-connector-kafka-3.3.0-1.20.jar",
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/kafka-clients-3.6.1.jar",
    )

    # Kafka source
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:19092")
        .set_topics("coinbase-ticker")
        .set_group_id("pyflink_ma5s")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Kafka sink with dynamic topic routing
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("localhost:19092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic_selector(topic_selector)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    # Pipeline
    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "coinbase_src")
        .map(parse,
             output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.LONG()]))
        .filter(lambda x: x is not None)
        # event-time & watermark (1 s out-of-order)
        .assign_timestamps_and_watermarks(
            WatermarkStrategy
            .for_bounded_out_of_orderness(Time.seconds(1))
            .with_timestamp_assigner(lambda rec, ts: rec[2])
        )
        .key_by(lambda r: r[0])                           # product_id
        .window(  SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
        .apply(MA5Window(), output_type=Types.STRING())
        .sink_to(sink)
    )

    env.execute("ma5s_per_symbol")


if __name__ == "__main__":
    main()
