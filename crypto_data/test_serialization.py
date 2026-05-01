#!/usr/bin/env python3
"""
Test script to validate serialization formats without requiring live Coinbase data.
"""

import json
from pathlib import Path
import polars as pl

# Test Polars schema and basic operations
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

def test_polars_parquet():
    """Test Polars Parquet read/write."""
    print("Testing Polars Parquet operations...")

    # Create sample data
    sample_data = [
        {
            "type": "ticker",
            "product_id": "BTC-USD",
            "price": 50000.0,
            "volume_24_h": 1000000.0,
            "low_24_h": 49000.0,
            "high_24_h": 51000.0,
            "low_52_w": "20000.0",
            "high_52_w": "69000.0",
            "price_percent_chg_24_h": 2.5,
            "best_bid": 49999.0,
            "best_ask": 50001.0,
            "best_bid_quantity": 1.5,
            "best_ask_quantity": 2.0,
            "last_size": 0.5,
            "volume_3d": 3000000.0,
            "open_24h": 49500.0,
            "parent_timestamp": "2025-01-01T00:00:00Z",
            "parent_sequence_num": 123456,
        }
    ]

    # Create DataFrame
    df = pl.DataFrame(sample_data, schema=POLARS_SCHEMA)
    print(f"✓ Created Polars DataFrame with {len(df)} rows")

    # Write to Parquet
    test_file = Path("test_output.parquet")
    df.write_parquet(test_file, compression="snappy", use_pyarrow=False)
    print(f"✓ Wrote Parquet file: {test_file}")

    # Read from Parquet
    df_read = pl.read_parquet(test_file)
    print(f"✓ Read Parquet file: {len(df_read)} rows")

    # Verify data
    assert len(df_read) == len(df), "Row count mismatch"
    assert df_read["price"][0] == 50000.0, "Price mismatch"
    print("✓ Data verification passed")

    # Cleanup
    test_file.unlink()
    print("✓ Cleaned up test file\n")

def test_avro_schema_validation():
    """Test Avro schema is valid JSON."""
    print("Testing Avro schema...")

    try:
        import fastavro

        # Validate schema
        parsed_schema = fastavro.parse_schema(AVRO_SCHEMA)
        print(f"✓ Avro schema is valid")
        print(f"  Schema name: {parsed_schema['name']}")
        print(f"  Fields: {len(parsed_schema['fields'])}")

        # Test serialization (without schema registry)
        sample_record = {
            "type": "ticker",
            "product_id": "BTC-USD",
            "price": 50000.0,
            "volume_24_h": 1000000.0,
            "low_24_h": 49000.0,
            "high_24_h": 51000.0,
            "low_52_w": "20000.0",
            "high_52_w": "69000.0",
            "price_percent_chg_24_h": 2.5,
            "best_bid": 49999.0,
            "best_ask": 50001.0,
            "best_bid_quantity": 1.5,
            "best_ask_quantity": 2.0,
            "last_size": 0.5,
            "volume_3d": 3000000.0,
            "open_24h": 49500.0,
            "parent_timestamp": "2025-01-01T00:00:00Z",
            "parent_sequence_num": 123456,
        }

        # Serialize
        from io import BytesIO
        output = BytesIO()
        fastavro.writer(output, parsed_schema, [sample_record])
        serialized = output.getvalue()
        print(f"✓ Serialized record to {len(serialized)} bytes")

        # Deserialize
        output.seek(0)
        records = list(fastavro.reader(output))
        assert len(records) == 1, "Should have one record"
        assert records[0]["product_id"] == "BTC-USD", "Product ID mismatch"
        print("✓ Deserialization successful\n")

    except ImportError:
        print("⚠ fastavro not installed, skipping Avro tests\n")

def test_protobuf_schema():
    """Test Protobuf schema exists."""
    print("Testing Protobuf schema...")

    proto_file = Path(__file__).parent / "coinbase_ticker.proto"
    if proto_file.exists():
        print(f"✓ Protobuf schema file exists: {proto_file}")

        # Check if compiled
        pb2_file = Path(__file__).parent / "coinbase_ticker_pb2.py"
        if pb2_file.exists():
            print(f"✓ Compiled Protobuf module exists: {pb2_file}")

            try:
                import coinbase_ticker_pb2

                # Create a test message
                msg = coinbase_ticker_pb2.CoinbaseTicker()
                msg.type = "ticker"
                msg.product_id = "BTC-USD"
                msg.price = 50000.0
                msg.volume_24_h = 1000000.0
                msg.low_24_h = 49000.0
                msg.high_24_h = 51000.0
                msg.low_52_w = "20000.0"
                msg.high_52_w = "69000.0"
                msg.price_percent_chg_24_h = 2.5
                msg.best_bid = 49999.0
                msg.best_ask = 50001.0
                msg.best_bid_quantity = 1.5
                msg.best_ask_quantity = 2.0
                msg.parent_timestamp = "2025-01-01T00:00:00Z"

                # Serialize
                serialized = msg.SerializeToString()
                print(f"✓ Serialized Protobuf message to {len(serialized)} bytes")

                # Deserialize
                msg2 = coinbase_ticker_pb2.CoinbaseTicker()
                msg2.ParseFromString(serialized)
                assert msg2.product_id == "BTC-USD", "Product ID mismatch"
                print("✓ Deserialization successful\n")

            except ImportError:
                print("⚠ Protobuf module not compiled. Run: ./setup_protobuf.sh\n")
        else:
            print("⚠ Protobuf schema not compiled. Run: ./setup_protobuf.sh\n")
    else:
        print("✗ Protobuf schema file not found\n")

def main():
    print("=" * 60)
    print("Coinbase Ticker Serialization Tests")
    print("=" * 60 + "\n")

    try:
        test_polars_parquet()
    except Exception as e:
        print(f"✗ Polars test failed: {e}\n")

    try:
        test_avro_schema_validation()
    except Exception as e:
        print(f"✗ Avro test failed: {e}\n")

    try:
        test_protobuf_schema()
    except Exception as e:
        print(f"✗ Protobuf test failed: {e}\n")

    print("=" * 60)
    print("Tests complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()
