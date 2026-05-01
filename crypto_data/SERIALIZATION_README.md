# Coinbase Ticker Serialization Formats

This tool supports multiple serialization formats for Kafka messages: JSON (default), Avro, and Protobuf.

## Installation

1. Install Python dependencies:
```bash
pip install -r ../requirements.txt
```

2. For Protobuf support, compile the schema:
```bash
./setup_protobuf.sh
```

Or manually:
```bash
# Install protobuf compiler
brew install protobuf  # macOS
# or
sudo apt-get install protobuf-compiler  # Ubuntu/Debian

# Compile the schema
protoc --python_out=. coinbase_ticker.proto
```

## Usage

### JSON Format (Default)
Stream to Kafka with JSON serialization:
```bash
python coinbase2parquet.py -k
```

### Avro Format
Stream to Kafka with Avro serialization and Schema Registry:
```bash
python coinbase2parquet.py -k --format avro --schema-registry-url http://localhost:18081
```

### Protobuf Format
Stream to Kafka with Protobuf serialization and Schema Registry:
```bash
python coinbase2parquet.py -k --format protobuf --schema-registry-url http://localhost:18081
```

## All Modes with Serialization

### Stream Coinbase → Kafka
```bash
# JSON (default)
python coinbase2parquet.py -k

# Avro
python coinbase2parquet.py -k --format avro

# Protobuf
python coinbase2parquet.py -k --format protobuf
```

### Stream Coinbase → Parquet (uses Polars)
```bash
python coinbase2parquet.py -F
python coinbase2parquet.py -F -o custom_output.parquet
```

### Read Parquet → Kafka (with serialization)
```bash
# JSON
python coinbase2parquet.py -FK -o coinbase_ticker_data.parquet

# Avro
python coinbase2parquet.py -FK -o coinbase_ticker_data.parquet --format avro

# Protobuf
python coinbase2parquet.py -FK -o coinbase_ticker_data.parquet --format protobuf
```

### Stream Coinbase → JSON Lines
```bash
python coinbase2parquet.py -J
python coinbase2parquet.py -J -o custom_output.jsonl
```

### Read JSON Lines → Kafka
```bash
python coinbase2parquet.py -JK -o coinbase_ticker_data.jsonl --format avro
```

### Stream Coinbase → SQLite
```bash
python coinbase2parquet.py -S
python coinbase2parquet.py -S -o custom_db.db
```

### Read SQLite → Kafka
```bash
python coinbase2parquet.py -SK -o coinbase_ticker_data.db --format protobuf
```

### Print Parquet Contents
```bash
python coinbase2parquet.py -PF -o coinbase_ticker_data.parquet
```

## Schema Registry Configuration

The default Schema Registry URL is `http://localhost:18081` (matches Redpanda setup).

To use a different Schema Registry:
```bash
python coinbase2parquet.py -k --format avro --schema-registry-url http://custom-host:8081
```

## Schemas

### Avro Schema
The Avro schema is defined inline in the script as `AVRO_SCHEMA` and automatically registered with the Schema Registry when using `--format avro`.

### Protobuf Schema
The Protobuf schema is defined in `coinbase_ticker.proto`. Compile it before using:
```bash
protoc --python_out=. coinbase_ticker.proto
```

This generates `coinbase_ticker_pb2.py` which is imported by the script.

## Performance Notes

- **Polars**: Replaced PyArrow/Pandas for faster Parquet operations and lower memory usage
- **Avro**: Compact binary format, excellent for data lakes and streaming
- **Protobuf**: Even more compact than Avro, best for high-throughput scenarios
- **JSON**: Human-readable, easiest to debug, but larger message size

## Troubleshooting

### "Protobuf module not found"
Run the setup script or manually compile:
```bash
./setup_protobuf.sh
```

### Schema Registry connection errors
Ensure Redpanda/Kafka is running and Schema Registry is accessible:
```bash
curl http://localhost:18081/subjects
```

### Import errors
Install all dependencies:
```bash
pip install -r ../requirements.txt
```
