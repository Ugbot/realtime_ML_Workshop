#!/usr/bin/env python3
"""
Stream Coinbase Advanced‑Trade tickers through an asyncio websocket.
Depending on flags, either pipe raw JSON to Kafka, append to a Parquet file,
or read from the Parquet file to Kafka.

Modes:
  -k   : Stream Coinbase → Kafka (default)
  -F   : Stream Coinbase → Parquet file (row‑group streaming)
  -FK  : Read Parquet file → Kafka
  -J   : Stream Coinbase → JSON Lines file
  -JK  : Read JSON Lines file → Kafka
  -S   : Stream Coinbase → SQLite database
  -SK  : Read SQLite database → Kafka
  -PF  : Print Parquet path + first 100 rows, then exit

Serialization formats for Kafka:
  --format json      : JSON serialization (default)
  --format avro      : Avro serialization with Schema Registry
  --format protobuf  : Protobuf serialization with Schema Registry
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import ssl
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, NoReturn, Optional

import certifi
import polars as pl
import websockets                      # pip install websockets
from confluent_kafka import Producer   # pip install confluent‑kafka
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from pydantic import BaseModel, Field, field_validator
import sqlite3
import fastavro

# ───────────────────────────────────────── Configuration ──
WS_URL = "wss://advanced-trade-ws.coinbase.com"
KAFKA_TOPIC = "coinbase-ticker"
PARQUET_TOPIC = "coinbase-ticker"
PARQUET_BATCH_SIZE = 50               # rows per row‑group
# Default filenames (used if -o is not provided)
DEFAULT_PARQUET_FILE = Path("./coinbase_ticker_data.parquet")
DEFAULT_JSONL_FILE = Path("./coinbase_ticker_data.jsonl")
DEFAULT_SQLITE_FILE = Path("./coinbase_ticker_data.db")
DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:18081"
PRODUCT_IDS = [
    "BTC-USD", "ETH-USD", "DOGE-USD", "XRP-USD",
    "LTC-USD", "BCH-USD", "ADA-USD", "SOL-USD",
    "DOT-USD", "LINK-USD", "XLM-USD", "UNI-USD",
    "ALGO-USD", "MATIC-USD",
]
# ─────────────────────────────────────────────────────────

# ─────────────────────────────────── Pydantic models ──
class TickerData(BaseModel):
    type: str
    product_id: str
    price: float
    volume_24_h: float
    low_24_h: float
    high_24_h: float
    low_52_w: str
    high_52_w: str
    price_percent_chg_24_h: float
    best_bid: float
    best_ask: float
    best_bid_quantity: float
    best_ask_quantity: float
    last_size: Optional[float] = None
    volume_3d: Optional[float] = None
    open_24h: Optional[float] = None
    parent_timestamp: str = Field(default="")
    parent_sequence_num: Optional[int] = None

    @field_validator(
        "price",
        "volume_24_h",
        "low_24_h",
        "high_24_h",
        "price_percent_chg_24_h",
        "best_bid",
        "best_ask",
        "best_bid_quantity",
        "best_ask_quantity",
        "last_size",
        "volume_3d",
        "open_24h",
        mode="before",
    )
    def _num(cls, v):
        if v in (None, ""):  # accept None / empty strings
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            logging.warning("Could not convert %r to float; setting to None", v)
            return None


class Ticker(BaseModel):
    type: str
    tickers: List[TickerData]


class Event(BaseModel):
    channel: str
    client_id: Optional[str] = None
    timestamp: str
    sequence_num: Optional[int] = None
    events: List[Ticker]

# ─────────────────────────────────────────────── Avro schema ──
AVRO_SCHEMA = {
    "type": "record",
    "name": "CoinbaseTicker",
    "namespace": "com.coinbase.ticker",
    "fields": [
        {"name": "type", "type": "string"},
        {"name": "product_id", "type": "string"},
        {"name": "price", "type": "double"},
        {"name": "volume_24_h", "type": "double"},
        {"name": "low_24_h", "type": "double"},
        {"name": "high_24_h", "type": "double"},
        {"name": "low_52_w", "type": "string"},
        {"name": "high_52_w", "type": "string"},
        {"name": "price_percent_chg_24_h", "type": "double"},
        {"name": "best_bid", "type": "double"},
        {"name": "best_ask", "type": "double"},
        {"name": "best_bid_quantity", "type": "double"},
        {"name": "best_ask_quantity", "type": "double"},
        {"name": "last_size", "type": ["null", "double"], "default": None},
        {"name": "volume_3d", "type": ["null", "double"], "default": None},
        {"name": "open_24h", "type": ["null", "double"], "default": None},
        {"name": "parent_timestamp", "type": "string"},
        {"name": "parent_sequence_num", "type": ["null", "long"], "default": None},
    ]
}

# ─────────────────────────────────────────────── Polars schema ──
POLARS_SCHEMA = {
    "type": pl.String,
    "product_id": pl.String,
    "price": pl.Float64,
    "volume_24_h": pl.Float64,
    "low_24_h": pl.Float64,
    "high_24_h": pl.Float64,
    "low_52_w": pl.String,
    "high_52_w": pl.String,
    "price_percent_chg_24_h": pl.Float64,
    "best_bid": pl.Float64,
    "best_ask": pl.Float64,
    "best_bid_quantity": pl.Float64,
    "best_ask_quantity": pl.Float64,
    "last_size": pl.Float64,
    "volume_3d": pl.Float64,
    "open_24h": pl.Float64,
    "parent_timestamp": pl.String,
    "parent_sequence_num": pl.Int64,
}

# ────────────────────────────────────────── SQLite setup ──
_SQLITE_TABLE_NAME = "coinbase_ticker"
_SQLITE_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {_SQLITE_TABLE_NAME} (
    type TEXT,
    product_id TEXT,
    price REAL,
    volume_24_h REAL,
    low_24_h REAL,
    high_24_h REAL,
    low_52_w TEXT,
    high_52_w TEXT,
    price_percent_chg_24_h REAL,
    best_bid REAL,
    best_ask REAL,
    best_bid_quantity REAL,
    best_ask_quantity REAL,
    last_size REAL,
    volume_3d REAL,
    open_24h REAL,
    parent_timestamp TEXT,
    parent_sequence_num INTEGER,
    -- Add a primary key for potential indexing/querying
    id INTEGER PRIMARY KEY AUTOINCREMENT
);
"""
_SQLITE_INSERT_SQL = f"INSERT INTO {_SQLITE_TABLE_NAME} (type, product_id, price, volume_24_h, low_24_h, high_24_h, low_52_w, high_52_w, price_percent_chg_24_h, best_bid, best_ask, best_bid_quantity, best_ask_quantity, last_size, volume_3d, open_24h, parent_timestamp, parent_sequence_num) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

def _get_sqlite_conn(db_path: Path) -> sqlite3.Connection:
    """Get SQLite connection and create table if needed."""
    conn = sqlite3.connect(db_path, check_same_thread=False) # Allow use in async context (with care)
    try:
        conn.execute(_SQLITE_SCHEMA)
        conn.commit()
        logging.debug("Ensured table %s exists in %s", _SQLITE_TABLE_NAME, db_path)
    except sqlite3.Error as e:
        logging.error("SQLite error during table creation: %s", e)
        conn.close() # Close connection if table creation fails
        raise
    return conn

# Global path, potentially updated by CLI args
_output_file_path: Path | None = None # Will be set in main()

# Global serialization configuration
_serialization_format: str = "json"
_schema_registry_client: SchemaRegistryClient | None = None
_avro_serializer: AvroSerializer | None = None
_protobuf_serializer: ProtobufSerializer | None = None

# ───────────────────────────────────────── SSL + logging ──
ssl_ctx = ssl.create_default_context(cafile=certifi.where())
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# ───────────────────────────────────────── WebSocket sub msg ──
SUBSCRIBE_MSG = json.dumps({
    "type": "subscribe",
    "product_ids": PRODUCT_IDS,
    "channel": "ticker",
})

# ───────────────────────────────────────── Serialization helpers ──
def _init_schema_registry(url: str) -> None:
    """Initialize Schema Registry client and serializers."""
    global _schema_registry_client, _avro_serializer, _protobuf_serializer

    if _schema_registry_client is None:
        logging.info("Connecting to Schema Registry at %s", url)
        _schema_registry_client = SchemaRegistryClient({"url": url})

    if _serialization_format == "avro" and _avro_serializer is None:
        logging.info("Initializing Avro serializer")
        _avro_serializer = AvroSerializer(
            _schema_registry_client,
            json.dumps(AVRO_SCHEMA),
            lambda obj, ctx: obj  # obj is already a dict
        )

    if _serialization_format == "protobuf" and _protobuf_serializer is None:
        logging.info("Initializing Protobuf serializer")
        # Import the generated protobuf module
        try:
            import coinbase_ticker_pb2
            _protobuf_serializer = ProtobufSerializer(
                coinbase_ticker_pb2.CoinbaseTicker,
                _schema_registry_client,
                conf={"use.deprecated.format": False}
            )
        except ImportError:
            logging.error("Protobuf module not found. Run: protoc --python_out=. coinbase_ticker.proto")
            raise

def _serialize_message(data: dict) -> bytes:
    """Serialize message based on configured format."""
    if _serialization_format == "json":
        return json.dumps(data).encode()
    elif _serialization_format == "avro":
        if _avro_serializer is None:
            raise RuntimeError("Avro serializer not initialized")
        ctx = SerializationContext(KAFKA_TOPIC, MessageField.VALUE)
        return _avro_serializer(data, ctx)
    elif _serialization_format == "protobuf":
        if _protobuf_serializer is None:
            raise RuntimeError("Protobuf serializer not initialized")
        try:
            import coinbase_ticker_pb2
            # Convert dict to protobuf message
            msg = coinbase_ticker_pb2.CoinbaseTicker(**data)
            ctx = SerializationContext(KAFKA_TOPIC, MessageField.VALUE)
            return _protobuf_serializer(msg, ctx)
        except ImportError:
            logging.error("Protobuf module not found")
            raise
    else:
        raise ValueError(f"Unknown serialization format: {_serialization_format}")

# ───────────────────────────────────────── Kafka producer ──
producer: Producer | None = None

def _get_producer() -> Producer:
    global producer
    if producer is None:
        logging.info("Initializing Kafka producer …")
        producer = Producer({"bootstrap.servers": "localhost:19092"})
    return producer

# ────────────────────────────────────────── Mode 1: CB → Kafka ──
async def stream_to_kafka() -> None:
    kafka = _get_producer()
    async with websockets.connect(WS_URL, ssl=ssl_ctx, ping_interval=20) as ws:
        await ws.send(SUBSCRIBE_MSG)
        logging.info("Subscribed; streaming to Kafka topic '%s' (format: %s)", KAFKA_TOPIC, _serialization_format)

        async for frame in ws:
            if _serialization_format == "json":
                # For JSON, send raw frame
                kafka.produce(KAFKA_TOPIC, value=frame.encode())
            else:
                # For Avro/Protobuf, parse and serialize
                try:
                    evt = Event.model_validate_json(frame)
                except Exception as err:
                    logging.debug("Parse error ignored: %s", err)
                    continue

                if evt.channel != "ticker":
                    continue

                for ev in evt.events:
                    if ev.type != "update":
                        continue
                    for tk in ev.tickers:
                        tk.parent_timestamp = evt.timestamp
                        tk.parent_sequence_num = evt.sequence_num
                        data = tk.model_dump()
                        try:
                            serialized = _serialize_message(data)
                            kafka.produce(KAFKA_TOPIC, value=serialized)
                        except Exception as e:
                            logging.error("Serialization error: %s", e)
                            continue

            kafka.poll(0)
            kafka.flush()

# ────────────────────────────────────────── Mode 2: CB → Parquet ──
async def stream_to_parquet() -> None:
    batch: list[TickerData] = []
    last_write = time.monotonic()
    first_write = True

    if _output_file_path.exists():
        logging.warning("%s already exists and will be **overwritten** when this run starts writing.", _output_file_path)

    async with websockets.connect(WS_URL, ssl=ssl_ctx, ping_interval=20) as ws:
        await ws.send(SUBSCRIBE_MSG)
        logging.info("Subscribed; streaming into Parquet file %s", _output_file_path)

        try:
            async for frame in ws:
                try:
                    evt = Event.model_validate_json(frame)
                except Exception as err:  # includes JSON/Pydantic errors
                    logging.debug("Parse error ignored: %s", err)
                    continue

                if evt.channel != "ticker":
                    continue

                for ev in evt.events:
                    if ev.type != "update":
                        continue
                    for tk in ev.tickers:
                        tk.parent_timestamp = evt.timestamp
                        tk.parent_sequence_num = evt.sequence_num
                        batch.append(tk)

                now = time.monotonic()
                need_flush = len(batch) >= PARQUET_BATCH_SIZE or (batch and now - last_write > 5)

                if need_flush:
                    _flush_batch_polars(batch, _output_file_path, first_write)
                    first_write = False
                    last_write = now
        finally:
            if batch:
                _flush_batch_polars(batch, _output_file_path, first_write)
            logging.info("Closed Parquet file %s", _output_file_path)


def _flush_batch_polars(batch: list[TickerData], file_path: Path, first_write: bool) -> None:
    """Write the current `batch` using Polars and clear it."""
    if not batch:
        return

    records = [t.model_dump() for t in batch]
    batch.clear()

    # Create Polars DataFrame
    df = pl.DataFrame(records, schema=POLARS_SCHEMA)

    # Write or append to Parquet
    if first_write:
        df.write_parquet(file_path, compression="snappy", use_pyarrow=False)
        logging.info("Created %s with %d rows", file_path, len(df))
    else:
        # Append mode - read existing, concatenate, write
        existing_df = pl.read_parquet(file_path)
        combined_df = pl.concat([existing_df, df])
        combined_df.write_parquet(file_path, compression="snappy", use_pyarrow=False)
        logging.info("Appended %d rows (total: %d rows, %.1f MiB)",
                     len(df), len(combined_df), file_path.stat().st_size / 2**20 if file_path.exists() else 0)

# ────────────────────────────────────────── Mode 4: CB → JSON Lines ──
async def stream_to_json() -> None:
    if _output_file_path.exists():
        logging.warning("%s already exists and will be **overwritten**.", _output_file_path)

    try:
        # Open in append mode with line buffering
        with open(_output_file_path, "a+", encoding="utf-8", buffering=1) as f:
            logging.info("Opened %s for writing JSON lines.", _output_file_path)
            async with websockets.connect(WS_URL, ssl=ssl_ctx, ping_interval=20) as ws:
                await ws.send(SUBSCRIBE_MSG)
                logging.info("Subscribed; streaming JSON lines into %s", _output_file_path)

                async for frame in ws:
                    try:
                        evt = Event.model_validate_json(frame)
                    except Exception as err:  # includes JSON/Pydantic errors
                        logging.debug("Parse error ignored: %s", err)
                        continue

                    if evt.channel != "ticker":
                        continue

                    for ev in evt.events:
                        if ev.type != "update":
                            continue
                        for tk in ev.tickers:
                            tk.parent_timestamp = evt.timestamp
                            tk.parent_sequence_num = evt.sequence_num
                            # Write each ticker as a JSON line
                            f.write(tk.model_dump_json() + '\n')

    except OSError as e:
        logging.error("Error opening or writing to %s: %s", _output_file_path, e)
    finally:
        logging.info("Stopped writing JSON Lines to %s", _output_file_path)

# ────────────────────────────────────────── Mode 3: Parquet → Kafka ──
async def read_parquet_to_kafka() -> None:
    if not _output_file_path.exists() or not _output_file_path.is_file():
        logging.error("Parquet file %s not found or is not a file", _output_file_path)
        return

    kafka = _get_producer()
    df = pl.read_parquet(_output_file_path)
    logging.info("Read %d rows; producing to Kafka (format: %s) …", len(df), _serialization_format)

    produced = 0
    for record in df.iter_rows(named=True):
        # Convert None to actual None for serialization
        record_clean = {k: (None if v is None else v) for k, v in record.items()}

        try:
            serialized = _serialize_message(record_clean)
            kafka.produce(KAFKA_TOPIC, value=serialized)
            produced += 1
            if produced % 1000 == 0:
                kafka.poll(0)
                logging.info("Produced %d records...", produced)
        except Exception as e:
            logging.error("Error producing record: %s", e)
            continue

    kafka.flush()
    logging.info("Finished sending %d records", produced)

# ────────────────────────────────────────── Mode 5: JSON Lines → Kafka ──
async def read_json_to_kafka() -> None:
    if not _output_file_path.exists() or not _output_file_path.is_file():
        logging.error("JSON file %s not found or is not a file", _output_file_path)
        return

    kafka = _get_producer()
    logging.info("Reading from %s and producing to Kafka topic '%s' ...", _output_file_path, KAFKA_TOPIC)

    produced = 0
    try:
        with open(_output_file_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    # No need to parse, just send the raw JSON line
                    kafka.produce(KAFKA_TOPIC, value=line.encode())
                    produced += 1
                    if produced % 1000 == 0:
                        kafka.poll(0)
                        logging.info("Produced %d records...", produced)
                except json.JSONDecodeError:
                    logging.warning("Skipping invalid JSON on line %d: %s", line_num, line[:100])
                except Exception as e:
                    logging.error("Error producing line %d: %s", line_num, e)
                    # Decide whether to continue or stop on other errors
                    # For now, log and continue

    except OSError as e:
        logging.error("Error reading file %s: %s", _output_file_path, e)
        return # Stop if we can't read the file

    kafka.flush()
    logging.info("Finished sending %d records from %s", produced, _output_file_path)

# ────────────────────────────────────────── Mode 6: CB → SQLite ──
async def stream_to_sqlite() -> None:
    batch: list[tuple] = []
    conn: sqlite3.Connection | None = None
    cursor: sqlite3.Cursor | None = None
    last_commit = time.monotonic()

    try:
        conn = _get_sqlite_conn(_output_file_path)
        cursor = conn.cursor()
        logging.info("Opened SQLite DB %s and ensured table '%s' exists.", _output_file_path, _SQLITE_TABLE_NAME)

        async with websockets.connect(WS_URL, ssl=ssl_ctx, ping_interval=20) as ws:
            await ws.send(SUBSCRIBE_MSG)
            logging.info("Subscribed; streaming records into %s", _output_file_path)

            async for frame in ws:
                try:
                    evt = Event.model_validate_json(frame)
                except Exception as err: # includes JSON/Pydantic errors
                    logging.debug("Parse error ignored: %s", err)
                    continue

                if evt.channel != "ticker":
                    continue

                for ev in evt.events:
                    if ev.type != "update":
                        continue
                    for tk in ev.tickers:
                        tk.parent_timestamp = evt.timestamp
                        tk.parent_sequence_num = evt.sequence_num
                        # Convert Pydantic model to tuple for insertion
                        # Ensure order matches _SQLITE_INSERT_SQL placeholders
                        record_tuple = (
                            tk.type,
                            tk.product_id,
                            tk.price,
                            tk.volume_24_h,
                            tk.low_24_h,
                            tk.high_24_h,
                            tk.low_52_w,
                            tk.high_52_w,
                            tk.price_percent_chg_24_h,
                            tk.best_bid,
                            tk.best_ask,
                            tk.best_bid_quantity,
                            tk.best_ask_quantity,
                            tk.last_size,
                            tk.volume_3d,
                            tk.open_24h,
                            tk.parent_timestamp,
                            tk.parent_sequence_num,
                        )
                        batch.append(record_tuple)

                now = time.monotonic()
                # Commit based on batch size or time interval
                need_commit = len(batch) >= 100 or (batch and now - last_commit > 5)

                if need_commit and cursor and conn:
                    try:
                        cursor.executemany(_SQLITE_INSERT_SQL, batch)
                        conn.commit()
                        logging.info("Committed %d records to SQLite", len(batch))
                        batch.clear()
                        last_commit = now
                    except sqlite3.Error as e:
                        logging.error("SQLite insert/commit error: %s", e)
                        # Consider rolling back or handling the error more robustly
                        # For now, clear batch to prevent retrying faulty data
                        batch.clear()

    except sqlite3.Error as e:
        logging.error("SQLite connection error: %s", e)
    except OSError as e:
        logging.error("Error accessing SQLite file %s: %s", _output_file_path, e)
    finally:
        # Final commit for any remaining items in batch
        if batch and cursor and conn:
            try:
                cursor.executemany(_SQLITE_INSERT_SQL, batch)
                conn.commit()
                logging.info("Committed final %d records to SQLite", len(batch))
            except sqlite3.Error as e:
                logging.error("SQLite final commit error: %s", e)
        if conn:
            conn.close()
            logging.info("Closed SQLite DB %s", _output_file_path)

# ────────────────────────────────────────── Mode 7: SQLite → Kafka ──
async def read_sqlite_to_kafka() -> None:
    if not _output_file_path.exists() or not _output_file_path.is_file():
        logging.error("SQLite file %s not found or is not a file", _output_file_path)
        return

    kafka = _get_producer()
    logging.info("Reading from SQLite DB %s (table: %s) and producing to Kafka topic '%s' ...",
                 _output_file_path, _SQLITE_TABLE_NAME, KAFKA_TOPIC)

    produced = 0
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(_output_file_path)
        conn.row_factory = sqlite3.Row # Fetch rows as dictionary-like objects
        cursor = conn.cursor()

        # Select all columns except the autoincrement id
        column_names = [f.name for f in PARQUET_SCHEMA] # Use Arrow schema fields for consistency
        query = f"SELECT {', '.join(column_names)} FROM {_SQLITE_TABLE_NAME}"
        cursor.execute(query)

        # Fetch rows in chunks to avoid loading large tables entirely into memory
        fetch_size = 1000
        while True:
            rows = cursor.fetchmany(fetch_size)
            if not rows:
                break

            for row in rows:
                try:
                    record_dict = dict(row)
                    # Convert to JSON string for Kafka
                    kafka.produce(KAFKA_TOPIC, value=json.dumps(record_dict).encode())
                    produced += 1
                    if produced % 1000 == 0:
                        kafka.poll(0)
                        logging.info("Produced %d records...", produced)
                except Exception as e:
                    logging.error("Error processing/producing row %s: %s", dict(row), e)
                    # Log and continue

    except sqlite3.Error as e:
        logging.error("SQLite error while reading: %s", e)
    except OSError as e:
        logging.error("Error accessing SQLite file %s: %s", _output_file_path, e)
    finally:
        if conn:
            conn.close()

    kafka.flush()
    logging.info("Finished sending %d records from SQLite DB %s", produced, _output_file_path)

# ────────────────────────────────────────── Utility: print parquet ──
async def print_parquet_contents() -> None:
    if not _output_file_path.exists() or not _output_file_path.is_file():
        logging.error("Parquet file not found or is not a file: %s", _output_file_path)
        return
    df = pl.read_parquet(_output_file_path)
    head = df.head(100)
    print(head.write_json(row_oriented=True))
    logging.info("Shown 100 / %d rows from %s", len(df), _output_file_path)

# ────────────────────────────────────────── main ──
async def main() -> None:
    p = argparse.ArgumentParser(description="Coinbase ticker streaming utility")
    p.add_argument("-PF", "--print-file", action="store_true",
                   help="Print Parquet path + first 100 rows, then exit")
    g = p.add_mutually_exclusive_group()
    g.add_argument("-k", "--kafka", action="store_true", help="Stream Coinbase → Kafka (default)")
    g.add_argument("-F", "--file", action="store_true", help="Stream Coinbase → Parquet file")
    g.add_argument("-FK", "--file-to-kafka", action="store_true", help="Read Parquet → Kafka")
    g.add_argument("-J", "--json", action="store_true", help="Stream Coinbase → JSON Lines file")
    g.add_argument("-JK", "--json-to-kafka", action="store_true", help="Read JSON Lines file → Kafka")
    g.add_argument("-S", "--sqlite", action="store_true", help="Stream Coinbase → SQLite database")
    g.add_argument("-SK", "--sqlite-to-kafka", action="store_true", help="Read SQLite database → Kafka")
    p.add_argument("-o", "--output-file", type=Path, default=None,
                   help="Input/Output file path for file/DB modes (-F, -J, -S, -FK, -JK, -SK, -PF). Default depends on mode.")
    p.add_argument("--format", choices=["json", "avro", "protobuf"], default="json",
                   help="Serialization format for Kafka messages (default: json)")
    p.add_argument("--schema-registry-url", type=str, default=DEFAULT_SCHEMA_REGISTRY_URL,
                   help=f"Schema Registry URL for Avro/Protobuf (default: {DEFAULT_SCHEMA_REGISTRY_URL})")
    args = p.parse_args()

    # Set global serialization format
    global _serialization_format
    _serialization_format = args.format

    # Initialize Schema Registry if using Avro or Protobuf
    if _serialization_format in ["avro", "protobuf"]:
        _init_schema_registry(args.schema_registry_url)

    # Determine mode first
    mode = (
        "file" if args.file else
        "file_to_kafka" if args.file_to_kafka else
        "json" if args.json else
        "json_to_kafka" if args.json_to_kafka else
        "sqlite" if args.sqlite else
        "sqlite_to_kafka" if args.sqlite_to_kafka else
        "kafka" # Default
    )

    # Set the output file path based on mode and -o argument
    global _output_file_path

    requires_file_arg = mode in ["file", "file_to_kafka", "json", "json_to_kafka", "sqlite", "sqlite_to_kafka"] or args.print_file

    if requires_file_arg:
        if args.output_file:
            _output_file_path = args.output_file
        else:
            # Assign default based on *output* mode if -o not given
            if mode == "file":
                _output_file_path = DEFAULT_PARQUET_FILE
            elif mode == "json":
                _output_file_path = DEFAULT_JSONL_FILE
            elif mode == "sqlite":
                _output_file_path = DEFAULT_SQLITE_FILE
            else:
                # Input modes require -o
                logging.error(f"Mode '{mode}' requires an input file path specified with -o.")
                return
    elif args.output_file:
        # -o provided but not needed for the mode (e.g., -k -o file.txt)
        logging.warning(f"Ignoring -o/--output-file argument ('{args.output_file}') as it's not used in mode '{mode}'.")

    if args.print_file:
        await print_parquet_contents()
        return

    logging.info("Selected mode: %s", mode)

    if mode == "file_to_kafka":
        await read_parquet_to_kafka()
        return
    if mode == "json_to_kafka":
        await read_json_to_kafka()
        return
    if mode == "sqlite_to_kafka":
        await read_sqlite_to_kafka()
        return

    # Select target function for streaming modes
    if mode == "file":
        target = stream_to_parquet
    elif mode == "json":
        target = stream_to_json
    elif mode == "sqlite":
        target = stream_to_sqlite
    else: # mode == "kafka" (default)
        target = stream_to_kafka

    # Check if _output_file_path is set for modes that need it
    # (This check should technically be redundant due to the logic above, but adds safety)
    if requires_file_arg and _output_file_path is None:
        logging.error(f"Internal error: Output file path not set for mode '{mode}'.")
        return

    backoff = 1

    while True:
        try:
            await target()
            logging.warning("Stream ended unexpectedly; restarting in %ds", backoff)
        except (asyncio.CancelledError, KeyboardInterrupt):
            break
        except (websockets.ConnectionClosedError, OSError) as exc:
            logging.warning("Connection error: %s; reconnecting in %ds", exc, backoff)
        except Exception as exc:
            logging.error("Unexpected error: %s; retrying in %ds", exc, backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Interrupted by user — shutting down …")
