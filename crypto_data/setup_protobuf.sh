#!/bin/bash
# Setup script for compiling Protobuf schema

echo "Installing protobuf compiler..."
# For macOS
if command -v brew &> /dev/null; then
    brew install protobuf
# For Ubuntu/Debian
elif command -v apt-get &> /dev/null; then
    sudo apt-get update && sudo apt-get install -y protobuf-compiler
# For other systems
else
    echo "Please install protobuf compiler manually: https://grpc.io/docs/protoc-installation/"
    exit 1
fi

echo "Compiling Protobuf schema..."
cd "$(dirname "$0")"
protoc --python_out=. coinbase_ticker.proto

echo "Protobuf setup complete!"
echo "Generated: coinbase_ticker_pb2.py"
