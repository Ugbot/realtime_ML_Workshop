# kafka_eth_rsi_windows.py  – RSI-14 with a sliding count window
import json
from pyflink.common import Types, WatermarkStrategy, SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import WindowFunction
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer, KafkaSource,
    KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
)


def parse_ticker(js: str):
    try:
        m = json.loads(js)
        t = m["events"][0]["tickers"][0]
        return t["product_id"], float(t["price"]), m["timestamp"]
    except Exception:
        return None


class RSIWindow(WindowFunction):
    def apply(self, key, window, inputs, out):  # ← self added
        rows = sorted(inputs, key=lambda r: r[2])
        closes = [r[1] for r in rows]

        gains = [max(0, closes[i + 1] - closes[i]) for i in range(13)]
        losses = [max(0, closes[i] - closes[i + 1]) for i in range(13)]
        avg_gain, avg_loss = sum(gains) / 14, sum(losses) / 14
        rsi = 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)

        j = json.dumps(
            {"product_id": key, "timestamp": rows[-1][2],
             "price": closes[-1], "rsi14": round(rsi, 4)},
            separators=(",", ":"))
        print(j, flush=True)
        out.collect(j)


def main():
    print("▶ ETH-USD RSI-14 sliding window…")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/flink-connector-kafka-3.3.0-1.20.jar",
        "file:///Users/bengamble/realtime_ML_Workshop/pyflink/kafka-clients-3.6.1.jar",
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:19092")
        .set_topics("coinbase-ticker")
        .set_group_id("pyflink_eth_rsi")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("localhost:19092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("eth-rsi")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "coinbase_src")
        .map(parse_ticker, output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.STRING()]))
        .filter(lambda x: x is not None and x[0] == "ETH-USD")
        .key_by(lambda r: r[0])
        .count_window(14, 1)
        .apply(RSIWindow(), output_type=Types.STRING())
        .sink_to(sink)
    )

    env.execute("eth_usd_rsi14_window")


if __name__ == "__main__":
    main()
