import asyncio
import multiprocessing
from time import sleep

from aiokafka import AIOKafkaProducer

from mastodon_types import MastodonPost
from mastodon_fetcher import MastodonFetcher

# Kafka server configuration - Updated to local Redpanda
kafka_bootstrap_servers = "localhost:19092"

# kafka_topic = "complete-json"
kafka_topic = "easy-timestamps" # Or choose another topic name

# Example callback function
async def handle_message(content: MastodonPost, producer):

    print(f"Received message: {content.json()}")

    # Send message to Kafka topic
    await producer.send_and_wait(kafka_topic, value=content.json().encode("utf-8"))

# Define a list of Mastodon servers
my_servers = [
    "https://mastodon.social",
    "https://hachyderm.io",
    "https://mastodon.cloud"
]

# Create a function to start a fetcher process for a server
def start_fetcher(server):
    # Create Kafka producer for local PLAINTEXT Redpanda
    async def run():
        # Removed security_protocol and ssl_context for PLAINTEXT
        producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
        await producer.start()
        try:
            fetcher = MastodonFetcher(server, 10,
                                      lambda content: asyncio.create_task(handle_message(content, producer)))
            await fetcher.run()
        finally:
            await producer.stop()

    asyncio.run(run())


# Create and start a process for each server
# def start_async_function(info):
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(start_fetcher(info))

processes = []

if __name__ == '__main__':
    for Mastodon_server in my_servers:
        sleep(1) # Add a small delay between starting processes
        process = multiprocessing.Process(target=start_fetcher, args=(Mastodon_server,))
        process.start()
        processes.append(process)

# Wait for all processes to complete
    for process in processes:
        process.join()
