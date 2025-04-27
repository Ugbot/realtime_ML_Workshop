#!/usr/bin/env python3
"""
Stream Coinbase Advanced-Trade tickers through an asyncio websocket.
Depending on flags, either pipe raw JSON to Kafka, append to a Parquet file,
or read from the Parquet file to Kafka.

Modes:
  -k : Stream Coinbase -> Kafka (default)
  -F : Stream Coinbase -> Parquet file
  -FK: Read Parquet file -> Kafka
"""

import asyncio
import argparse
import json
import ssl
import time
import sys
import functools
from pathlib import Path
from typing import NoReturn, List, Dict, Any

import certifi
import websockets                       # pip install websockets
import pandas as pd                     # pip install pandas pyarrow
import pyarrow                          # pip install pyarrow
from confluent_kafka import Producer    # pip install confluent-kafka

# --- Configuration ---
WS_URL = "wss://advanced-trade-ws.coinbase.com"
KAFKA_TOPIC = "coinbase-ticker"
PARQUET_FILE = Path("./coinbase_ticker_data.parquet")
# Set batch size for writing to Parquet
PARQUET_BATCH_SIZE = 100
# List of product IDs to subscribe to
PRODUCT_IDS = [
    "BTC-USD", "ETH-USD", "DOGE-USD", "XRP-USD",
    "LTC-USD", "BCH-USD", "ADA-USD", "SOL-USD",
    "DOT-USD", "LINK-USD", "XLM-USD", "UNI-USD",
    "ALGO-USD", "MATIC-USD",
]
# --- End Configuration ---

ssl_ctx = ssl.create_default_context(cafile=certifi.where())
# Initialize producer to None, create only if needed
producer: Producer | None = None

SUBSCRIBE_MSG = json.dumps(
    {
        "type": "subscribe",
        "product_ids": PRODUCT_IDS,
        "channel": "ticker",
    }
)


def _get_producer() -> Producer:
    """Creates or returns the existing Kafka producer."""
    global producer
    if producer is None:
        print("Initializing Kafka producer...")
        producer = Producer({"bootstrap.servers": "localhost:19092"})
    return producer


async def stream_to_kafka() -> None:
    """Connect to WebSocket, subscribe, forward every frame to Kafka."""
    kafka_producer = _get_producer()
    async with websockets.connect(WS_URL, ssl=ssl_ctx, ping_interval=20) as ws:
        await ws.send(SUBSCRIBE_MSG)
        print(f"Subscribed to ticker channel. Streaming to Kafka topic '{KAFKA_TOPIC}'...")

        async for frame in ws:
            try:
                # Forward raw JSON to Kafka
                kafka_producer.produce(KAFKA_TOPIC, value=frame.encode())
                kafka_producer.poll(0)  # Non-blocking flush
                # Optional: decode for logging
                # data = json.loads(frame)
                # print(f"Sent to Kafka: {data.get('product_id')} @ {data.get('price')}")
            except json.JSONDecodeError:
                print(f"Ignoring non-JSON frame: {frame[:100]}...")
            except Exception as e:
                print(f"Error producing to Kafka: {e}")


async def stream_to_parquet() -> None:
    """Connect to WebSocket, subscribe, batch data, append to Parquet file."""
    batch: List[Dict[str, Any]] = []
    last_write_time = time.monotonic()

    async with websockets.connect(WS_URL, ssl=ssl_ctx, ping_interval=20) as ws:
        await ws.send(SUBSCRIBE_MSG)
        print(f"Subscribed to ticker channel. Appending data to '{PARQUET_FILE}'...")

        try:
            async for frame in ws:
                try:
                    data = json.loads(frame)
                    # Ensure it's ticker data we expect (basic check)
                    if data.get("type") == "ticker" and "product_id" in data:
                        batch.append(data)

                    current_time = time.monotonic()
                    # Write batch if size reached or timeout (e.g., 5 seconds)
                    if len(batch) >= PARQUET_BATCH_SIZE or (batch and current_time - last_write_time > 5):
                        if not batch: continue # Skip if batch became empty somehow

                        df = pd.DataFrame(batch)
                        try:
                            # Convert known numeric fields, handle errors gracefully
                            for col in ['price', 'open_24h', 'volume_24h', 'low_24h', 'high_24h', 'volume_3d',
                                        'best_bid', 'best_bid_size', 'best_ask', 'best_ask_size', 'last_size']:
                                if col in df.columns:
                                    df[col] = pd.to_numeric(df[col], errors='coerce')
                            # Convert time to datetime
                            if 'time' in df.columns:
                                df['time'] = pd.to_datetime(df['time'], errors='coerce')

                            # Append DataFrame to Parquet file
                            df.to_parquet(PARQUET_FILE, engine='pyarrow', append=True, index=False)
                            print(f"Appended batch of {len(batch)} records to {PARQUET_FILE}")
                            batch = [] # Clear batch
                            last_write_time = current_time
                        except (pd.errors.ParserError, pyarrow.lib.ArrowInvalid, ValueError) as write_err:
                             print(f"Error processing or writing batch to Parquet: {write_err}. Skipping batch.")
                             print(f"Problematic batch head: {batch[:5]}")
                             batch = [] # Clear problematic batch
                        except Exception as write_err:
                             print(f"Unexpected error writing batch to Parquet: {write_err}. Skipping batch.")
                             print(f"Problematic batch head: {batch[:5]}")
                             batch = [] # Clear problematic batch


                except json.JSONDecodeError:
                    print(f"Ignoring non-JSON frame: {frame[:100]}...")
                except Exception as e:
                    print(f"Error processing frame: {e}")
        finally:
            # Write any remaining records on exit
            if batch:
                print(f"Writing final batch of {len(batch)} records...")
                df = pd.DataFrame(batch)
                try:
                     # Convert known numeric fields, handle errors gracefully
                    for col in ['price', 'open_24h', 'volume_24h', 'low_24h', 'high_24h', 'volume_3d',
                                'best_bid', 'best_bid_size', 'best_ask', 'best_ask_size', 'last_size']:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    if 'time' in df.columns:
                        df['time'] = pd.to_datetime(df['time'], errors='coerce')

                    df.to_parquet(PARQUET_FILE, engine='pyarrow', append=True, index=False)
                    print(f"Final batch appended to {PARQUET_FILE}")
                except Exception as write_err:
                     print(f"Error writing final batch to Parquet: {write_err}")
                     print(f"Final batch head: {batch[:5]}")


async def read_parquet_to_kafka() -> None:
    """Read data from Parquet file and produce it to Kafka."""
    if not PARQUET_FILE.exists():
        print(f"Error: Parquet file not found at '{PARQUET_FILE}'")
        return

    print(f"Reading from '{PARQUET_FILE}' and producing to Kafka topic '{KAFKA_TOPIC}'...")
    try:
        df = pd.read_parquet(PARQUET_FILE, engine='pyarrow')
        print(f"Read {len(df)} records from Parquet.")

        records_produced = 0
        for record in df.to_dict('records'):
             # Convert NaNs/NaTs to None for JSON compatibility
             # Also convert Timestamp back to ISO 8601 string format if needed
             record_cleaned = {}
             for key, value in record.items():
                 if pd.isna(value):
                     record_cleaned[key] = None
                 elif isinstance(value, pd.Timestamp):
                      record_cleaned[key] = value.isoformat()
                 else:
                     record_cleaned[key] = value

             try:
                 producer.produce(KAFKA_TOPIC, value=json.dumps(record_cleaned).encode())
                 records_produced += 1
                 # Poll occasionally to prevent buffer buildup and handle delivery reports
                 if records_produced % 1000 == 0:
                     producer.poll(0)
                     print(f"Produced {records_produced} records...")

             except BufferError:
                 print("Kafka producer queue full. Flushing...")
                 producer.flush() # Block until messages delivered/failed
                 # Retry producing the message
                 producer.produce(KAFKA_TOPIC, value=json.dumps(record_cleaned).encode())
                 records_produced += 1
             except Exception as e:
                 print(f"Error producing message to Kafka: {e}")
                 print(f"Problematic record: {record_cleaned}")

        print(f"Finished producing {records_produced} records from Parquet to Kafka.")

    except Exception as e:
        print(f"Error reading Parquet file or producing to Kafka: {e}")
    finally:
        print("Flushing final Kafka messages...")
        producer.flush() # Ensure all messages are sent before exiting
        print("Kafka flush complete.")


async def print_parquet_contents() -> None:
    """Prints the Parquet file path and its first 100 rows as JSON lines."""
    abs_path = PARQUET_FILE.resolve()
    print(f"Parquet file path: {abs_path}")

    if not abs_path.exists():
        print("Error: Parquet file not found.")
        return

    try:
        print(f"Reading first 100 rows from {abs_path}...")
        df = pd.read_parquet(abs_path, engine='pyarrow')
        df_head = df.head(100)

        if df_head.empty:
            print("Parquet file is empty.")
            return

        print("--- Start of File Contents (first 100 rows as JSON) ---")
        # Convert Timestamps to ISO format strings for JSON
        for col in df_head.select_dtypes(include=['datetime64[ns]']).columns:
            df_head[col] = df_head[col].dt.isoformat()
        # Handle potential NaNs which are not valid JSON
        json_output = df_head.to_json(orient='records', lines=True, date_format='iso', default_handler=str)
        print(json_output)
        print("--- End of File Contents ---")
        print(f"Total rows in file: {len(df)}")

    except Exception as e:
        print(f"Error reading or printing Parquet file: {e}")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Coinbase data streaming utility.")
    parser.add_argument("-PF", "--print-file", action="store_true",
                        help="Print Parquet file path and first 100 rows as JSON, then exit.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-k", "--kafka", action="store_true",
                       help="Stream data directly from Coinbase to Kafka (default).")
    group.add_argument("-F", "--file", action="store_true",
                       help="Stream data from Coinbase and append to Parquet file.")
    group.add_argument("-FK", "--file-to-kafka", action="store_true",
                       help="Read data from Parquet file and send to Kafka.")

    args = parser.parse_args()

    # Handle -PF flag first
    if args.print_file:
        await print_parquet_contents()
        return # Exit after printing

    # Determine mode if -PF was not used
    mode = "kafka" # Default mode
    if args.file:
        mode = "file"
    elif args.file_to_kafka:
        mode = "file_to_kafka"

    print(f"Selected mode: {mode}")

    if mode == "file_to_kafka":
        # Run once, no retry loop
        await read_parquet_to_kafka()
    else:
        # Run streaming modes with retry loop and quit option
        target_func = stream_to_parquet if mode == "file" else stream_to_kafka
        backoff = 1
        loop = asyncio.get_running_loop()
        should_stop = False

        print(f"Starting stream mode '{mode}'. Press 'q' then Enter to quit gracefully.")

        while not should_stop:
            stream_task = None
            input_task = None
            try:
                print(f"Attempting to connect and start streaming (backoff: {backoff}s)...")
                stream_task = asyncio.create_task(target_func())
                # Run blocking stdin read in executor
                input_task = loop.run_in_executor(None, sys.stdin.readline)

                done, pending = await asyncio.wait(
                    [stream_task, input_task],
                    return_when=asyncio.FIRST_COMPLETED
                )

                if input_task in done:
                    user_input = await input_task # Get result from input task
                    if user_input.strip().lower() == 'q':
                        print("'q' received. Shutting down gracefully...")
                        should_stop = True
                        if stream_task in pending:
                            stream_task.cancel()
                            await asyncio.wait([stream_task], timeout=10) # Wait for cancellation
                    else:
                         print(f"Input '{user_input.strip()}' ignored. Press 'q' then Enter to quit.")
                         # Restart loop immediately if non-'q' input received
                         if stream_task in pending:
                             stream_task.cancel() # Cancel current stream before restarting loop
                             await asyncio.wait([stream_task], timeout=5)


                if stream_task in done:
                    # If stream task finished, it likely encountered an error
                    try:
                        await stream_task # Raise exception if one occurred
                        # If no exception, stream ended unexpectedly
                        print("Stream function finished unexpectedly. Will restart.")
                        backoff = 1 # Reset backoff
                    except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
                        print(f"Websocket/Connection error: {exc!s}. Reconnecting in {backoff}s ...")
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 60)
                    except Exception as e: # Catch other potential errors from stream_task
                        print(f"Unexpected error in stream task: {e}. Restarting in {backoff}s...")
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 60)

            except KeyboardInterrupt:
                print("\nInterrupted by user (Ctrl+C). Exiting.")
                should_stop = True
            except asyncio.CancelledError:
                 print("Stream task cancelled.")
                 should_stop = True # Ensure loop terminates if cancelled externally
            except Exception as e:
                 print(f"Unexpected error in main loop management: {e}. Restarting in {backoff}s...")
                 await asyncio.sleep(backoff)
                 backoff = min(backoff * 2, 60) # Also backoff on unexpected errors
            finally:
                # Cleanup tasks before next iteration or exit
                if stream_task and not stream_task.done():
                    stream_task.cancel()
                    try:
                        await asyncio.wait_for(stream_task, timeout=5.0)
                    except asyncio.TimeoutError:
                        print("Stream task did not cancel in time.")
                    except asyncio.CancelledError:
                        pass # Expected
                if input_task and not input_task.done():
                    # Input task is harder to cancel cleanly, but it's non-critical if it lingers
                    # It might block the executor thread until newline is received
                     pass
                # Flush producer after each attempt/cycle or before exiting
                if producer: # Only flush if producer was initialized
                    print("Flushing Kafka producer...")
                    producer.flush(timeout=5)

        print("Exited streaming loop.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nMain loop interrupted. Exiting.")
    finally:
        # Final flush just in case
        if producer: # Only flush if producer was initialized
            print("Final Kafka producer flush...")
            producer.flush(timeout=10)
        print("Exited.")
