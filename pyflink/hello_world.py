# hello_world_datastream.py
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types


def main():
    # 1. Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)          # keep it single-task for clarity

    # 2. Create a small source
    ds = env.from_collection(
        collection=["Hello World – PyFlink DataStream 🎉"],
        type_info=Types.STRING()
    )

    # 3. Output the records
    ds.print()                      # prints to stdout (or taskmanager logs on a cluster)

    # 4. Trigger execution
    env.execute("hello_world_datastream_job")


if __name__ == "__main__":
    main()
