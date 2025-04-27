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
import logging
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
PARQUET_BATCH_SIZE = 50
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

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout) # Log to standard output
    ]
)
# --- End Logging Setup ---

def _get_producer() -> Producer:
    """Creates or returns the existing Kafka producer."""
    global producer
    if producer is None:
        logging.info("Initializing Kafka producer...")
        producer = Producer({"bootstrap.servers": "localhost:19092"})
    return producer


async def stream_to_kafka() -> None:
    """Connect to WebSocket, subscribe, forward every frame to Kafka."""
    kafka_producer = _get_producer()
    async with websockets.connect(WS_URL, ssl=ssl_ctx, ping_interval=20) as ws:
        await ws.send(SUBSCRIBE_MSG)
        logging.info(f"Subscribed to ticker channel. Streaming to Kafka topic '{KAFKA_TOPIC}'...")

        async for frame in ws:
            try:
                # Forward raw JSON to Kafka
                kafka_producer.produce(KAFKA_TOPIC, value=frame.encode())
                kafka_producer.poll(0)  # Non-blocking flush
                # Optional: decode for logging
                # data = json.loads(frame)
                # logging.debug(f"Sent to Kafka: {data.get('product_id')} @ {data.get('price')}")
            except json.JSONDecodeError:
                logging.warning(f"Ignoring non-JSON frame: {frame[:100]}...")
            except Exception as e:
                logging.error(f"Error producing to Kafka: {e}")


async def stream_to_parquet() -> None:
    """Connect to WebSocket, subscribe, batch data, append to Parquet file."""
    batch: List[Dict[str, Any]] = []
    last_write_time = time.monotonic()
    record_count = 0
    # Check if the Parquet file already exists before starting
    parquet_file_exists = PARQUET_FILE.exists()
    if parquet_file_exists:
        logging.info(f"Parquet file '{PARQUET_FILE}' already exists. Will append.")
    else:
        logging.info(f"Parquet file '{PARQUET_FILE}' does not exist. Will create on first batch.")

    async with websockets.connect(WS_URL, ssl=ssl_ctx, ping_interval=20) as ws:
        await ws.send(SUBSCRIBE_MSG)
        logging.info(f"Subscribed to ticker channel. Appending data to '{PARQUET_FILE}'...")

        try:
            async for frame in ws:
                # logging.debug(f"Raw frame received: {frame[:200]}...") # Uncomment for verbose debugging
                try:
                    data = json.loads(frame)
                    # Ensure it's ticker data and flatten the structure
                    if data.get("channel") == "ticker" and "events" in data:
                        for event in data.get("events", []):
                            if event.get("type") == "update" and "tickers" in event:
                                for ticker_data in event.get("tickers", []):
                                    if ticker_data.get("type") == "ticker" and "product_id" in ticker_data:
                                        # Add the actual ticker data dictionary to the batch
                                        # Include timestamp and sequence_num from the parent message if needed
                                        ticker_data['parent_timestamp'] = data.get('timestamp')
                                        ticker_data['parent_sequence_num'] = data.get('sequence_num')
                                        batch.append(ticker_data)
                                        record_count += 1
                                        logging.debug(f"Added flattened record {record_count} ({ticker_data.get('product_id')}) to batch (current size: {len(batch)})")
                            else:
                                logging.debug(f"Ignoring event type '{event.get('type')}' or missing 'tickers' key in event: {event}")
                    else:
                        logging.debug(f"Ignoring message channel '{data.get('channel')}' or missing 'events' key. Data: {data}")

                    current_time = time.monotonic()
                    write_reason = None
                    if len(batch) >= PARQUET_BATCH_SIZE:
                         write_reason = f"batch size limit ({PARQUET_BATCH_SIZE}) reached"
                    elif batch and current_time - last_write_time > 5:
                         write_reason = f"time limit (5s) reached"

                    # Write batch if size reached or timeout
                    if write_reason:
                        if not batch: continue # Skip if batch became empty somehow
                        batch_to_write = batch[:] # Create a copy for writing
                        batch = [] # Clear original batch immediately
                        logging.info(f"Attempting to write batch of {len(batch_to_write)} records (Reason: {write_reason})...")

                        try:
                            df = pd.DataFrame(batch_to_write)
                            # Convert known numeric fields, handle errors gracefully
                            for col in ['price', 'open_24h', 'volume_24h', 'low_24h', 'high_24h', 'volume_3d',
                                        'best_bid', 'best_bid_size', 'best_ask', 'best_ask_size', 'last_size']:
                                if col in df.columns:
                                    df[col] = pd.to_numeric(df[col], errors='coerce')
                            # Convert time to datetime
                            if 'time' in df.columns:
                                df['time'] = pd.to_datetime(df['time'], errors='coerce')

                            # Append/Create DataFrame to Parquet file
                            if not parquet_file_exists:
                                logging.debug(f"Executing df.to_parquet (creation) for {len(df)} rows...")
                                df.to_parquet(PARQUET_FILE, engine='pyarrow', index=False)
                                logging.info(f"Successfully CREATED Parquet file '{PARQUET_FILE}' with batch of {len(df)} records.")
                                parquet_file_exists = True # Mark as existing now
                            else:
                                logging.debug(f"Executing df.to_parquet (append) for {len(df)} rows...")
                                df.to_parquet(PARQUET_FILE, engine='pyarrow', append=True, index=False)
                                logging.info(f"Successfully APPENDED batch of {len(df)} records to {PARQUET_FILE}")

                            last_write_time = current_time
                        except (pd.errors.ParserError, pyarrow.lib.ArrowInvalid, ValueError) as write_err:
                             logging.error(f"Error processing or writing batch to Parquet: {write_err}. Skipping batch.")
                        except Exception as write_err:
                             logging.error(f"Unexpected error writing batch to Parquet: {write_err}. Skipping batch.")
                             logging.error(f"Problematic batch head (first 5 records):\n{batch_to_write[:5]}")


                except json.JSONDecodeError:
                    logging.warning(f"Ignoring non-JSON frame: {frame[:100]}...")
                except Exception as e:
                    logging.error(f"Error processing frame: {e}")
        finally:
            # Write any remaining records on exit
            if batch:
                logging.info(f"Writing final batch of {len(batch)} records...")
                batch_to_write = batch[:]
                batch = [] # Clear original batch
                try:
                    df = pd.DataFrame(batch_to_write)
                     # Convert known numeric fields, handle errors gracefully
                    for col in ['price', 'open_24h', 'volume_24h', 'low_24h', 'high_24h', 'volume_3d',
                                'best_bid', 'best_bid_size', 'best_ask', 'best_ask_size', 'last_size']:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    if 'time' in df.columns:
                        df['time'] = pd.to_datetime(df['time'], errors='coerce')

                    # Write/Append logic for final batch
                    if not parquet_file_exists:
                        logging.debug(f"Executing df.to_parquet (creation) for final batch of {len(df)} rows...")
                        df.to_parquet(PARQUET_FILE, engine='pyarrow', index=False)
                        logging.info(f"Successfully CREATED Parquet file '{PARQUET_FILE}' with final batch of {len(df)} records.")
                        parquet_file_exists = True # Mark as existing now
                    else:
                        logging.debug(f"Executing df.to_parquet (append) for final batch of {len(df)} rows...")
                        df.to_parquet(PARQUET_FILE, engine='pyarrow', append=True, index=False)
                        logging.info(f"Successfully APPENDED final batch of {len(df)} records to {PARQUET_FILE}")

                except Exception as write_err:
                     logging.error(f"Error writing final batch to Parquet: {write_err}")
                     logging.error(f"Final batch head (first 5 records):\n{batch_to_write[:5]}")
            else:
                logging.info("No final batch to write.")


async def read_parquet_to_kafka() -> None:
    """Read data from Parquet file and produce it to Kafka."""
    kafka_producer = _get_producer()
    if not PARQUET_FILE.exists():
        logging.error(f"Parquet file not found at '{PARQUET_FILE}'")
        return

    logging.info(f"Reading from '{PARQUET_FILE}' and producing to Kafka topic '{KAFKA_TOPIC}'...")
    try:
        df = pd.read_parquet(PARQUET_FILE, engine='pyarrow')
        logging.info(f"Read {len(df)} records from Parquet.")

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
                 kafka_producer.produce(KAFKA_TOPIC, value=json.dumps(record_cleaned).encode())
                 records_produced += 1
                 # Poll occasionally to prevent buffer buildup and handle delivery reports
                 if records_produced % 1000 == 0:
                     kafka_producer.poll(0)
                     logging.info(f"Produced {records_produced} records...")

             except BufferError:
                 logging.warning("Kafka producer queue full. Flushing...")
                 kafka_producer.flush() # Block until messages delivered/failed
                 # Retry producing the message
                 kafka_producer.produce(KAFKA_TOPIC, value=json.dumps(record_cleaned).encode())
                 records_produced += 1
             except Exception as e:
                 logging.error(f"Error producing message to Kafka: {e}")
                 logging.error(f"Problematic record: {record_cleaned}")

        logging.info(f"Finished producing {records_produced} records from Parquet to Kafka.")

    except Exception as e:
        logging.error(f"Error reading Parquet file or producing to Kafka: {e}")
    finally:
        logging.info("Flushing final Kafka messages...")
        kafka_producer.flush() # Ensure all messages are sent before exiting
        logging.info("Kafka flush complete.")


async def print_parquet_contents() -> None:
    """Prints the Parquet file path and its first 100 rows as JSON lines."""
    abs_path = PARQUET_FILE.resolve()
    logging.info(f"Parquet file path: {abs_path}")

    if not abs_path.exists():
        logging.error("Error: Parquet file not found.")
        return

    try:
        logging.info(f"Reading first 100 rows from {abs_path}...")
        df = pd.read_parquet(abs_path, engine='pyarrow')
        df_head = df.head(100)

        if df_head.empty:
            logging.info("Parquet file is empty.")
            return

        logging.info("--- Start of File Contents (first 100 rows as JSON) ---")
        # Convert Timestamps to ISO format strings for JSON
        for col in df_head.select_dtypes(include=['datetime64[ns]']).columns:
            df_head[col] = df_head[col].dt.isoformat()
        # Handle potential NaNs which are not valid JSON
        json_output = df_head.to_json(orient='records', lines=True, date_format='iso', default_handler=str)
        print(json_output) # Keep print here for direct output
        logging.info("--- End of File Contents ---")
        logging.info(f"Total rows in file: {len(df)}")

    except Exception as e:
        logging.error(f"Error reading or printing Parquet file: {e}")


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

        logging.info(f"Starting stream mode '{mode}'. Press 'q' then Enter to quit gracefully.")

        while not should_stop:
            stream_task = None
            input_task = None
            try:
                logging.info(f"Attempting to connect and start streaming (backoff: {backoff}s)...")
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
                        logging.info("'q' received. Shutting down gracefully...")
                        should_stop = True
                        if stream_task in pending:
                            stream_task.cancel()
                            try:
                                await asyncio.wait_for(stream_task, timeout=10) # Wait for cancellation
                            except (asyncio.TimeoutError, asyncio.CancelledError):
                                logging.info("Stream task cancelled or timed out during shutdown.")
                    else:
                        logging.warning(f"Input '{user_input.strip()}' ignored. Press 'q' then Enter to quit.")
                          # Restart loop immediately if non-'q' input received
                        if stream_task in pending:
                            stream_task.cancel() # Cancel current stream before restarting loop
                            try:
                                await asyncio.wait_for(stream_task, timeout=5)
                            except (asyncio.TimeoutError, asyncio.CancelledError):
                                logging.debug("Stream task cancelled or timed out during restart.")


                if stream_task in done:
                    # If stream task finished, it likely encountered an error
                    try:
                        await stream_task # Raise exception if one occurred
                        # If no exception, stream ended unexpectedly
                        logging.warning("Stream function finished unexpectedly. Will restart.")
                        backoff = 1 # Reset backoff
                    except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
                        logging.warning(f"Websocket/Connection error: {exc!s}. Reconnecting in {backoff}s ...")
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 60)
                    except Exception as e: # Catch other potential errors from stream_task
                        logging.error(f"Unexpected error in stream task: {e}. Restarting in {backoff}s...")
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 60)

            except KeyboardInterrupt:
                logging.info("\nInterrupted by user (Ctrl+C). Exiting.")
                should_stop = True
            except asyncio.CancelledError:
                logging.info("Stream task cancelled.")
                should_stop = True # Ensure loop terminates if cancelled externally
            except Exception as e:
                logging.error(f"Unexpected error in main loop management: {e}. Restarting in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60) # Also backoff on unexpected errors
            finally:
                # Cleanup tasks before next iteration or exit
                if stream_task and not stream_task.done():
                    stream_task.cancel()
                    try:
                        await asyncio.wait_for(stream_task, timeout=5.0)
                    except asyncio.TimeoutError:
                        logging.warning("Stream task did not cancel in time.")
                    except asyncio.CancelledError:
                        pass # Expected
                if input_task and not input_task.done():
                    # Input task is harder to cancel cleanly, but it's non-critical if it lingers
                    # It might block the executor thread until newline is received
                    pass
                # Flush producer after each attempt/cycle or before exiting
                if producer: # Only flush if producer was initialized
                    logging.info("Flushing Kafka producer...")
                    producer.flush(timeout=5)

        logging.info("Exited streaming loop.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\nMain loop interrupted. Exiting.")
    finally:
        # Final flush just in case
        if producer: # Only flush if producer was initialized
            logging.info("Final Kafka producer flush...")
            producer.flush(timeout=10)
        logging.info("Script finished.")
