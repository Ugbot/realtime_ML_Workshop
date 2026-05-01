"""
Step 01 – Hello World (local DataStream)
========================================
Concepts:  StreamExecutionEnvironment, from_collection, map, filter, print, execute

No Kafka, no JARs – just the Flink runtime.

Run:
    uv run python 01_hello_world.py
"""
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # A bounded local source – useful for unit-testing pipelines offline.
    prices = env.from_collection(
        collection=[3100.0, 3105.5, 3098.2, 3112.0, 3089.9, 3120.1],
        type_info=Types.DOUBLE(),
    )

    # Every operator returns a new DataStream; nothing runs until execute().
    (
        prices
        .map(lambda p: round(p, 1), output_type=Types.DOUBLE())
        .filter(lambda p: p > 3100.0)
        .map(lambda p: f"ETH-USD  ${p:,.1f}", output_type=Types.STRING())
        .print()
    )

    # execute() submits the DAG and blocks until the job finishes.
    env.execute("01_hello_world")


if __name__ == "__main__":
    main()
