import websocket
import json
from confluent_kafka import Producer
# Market Data WebSocket URL
WS_URL = "wss://advanced-trade-ws.coinbase.com"

my_producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

def on_open(ws):
    # Subscribe to the ticker channel for BTC-USD
    subscribe_message = {
        "type": "subscribe",
        "product_ids": [
            "BTC-USD", "ETH-USD", "DOGE-USD", "XRP-USD",  # existing pairs
            "LTC-USD", "BCH-USD", "ADA-USD", "SOL-USD", "DOT-USD",
            "LINK-USD", "XLM-USD", "UNI-USD", "ALGO-USD", "MATIC-USD"
        ],
        "channel": "ticker"
    }
    ws.send(json.dumps(subscribe_message))
    print("Subscribed to BTC-USD ticker channel")

def on_message(ws, message):
    data = json.loads(message)
    # Send the message to Kafka
    my_producer.produce("coinbase-ticker", value=message)
    print(f"Received message: {data}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws):
    print("Connection closed")

# Create the WebSocket connection
ws = websocket.WebSocketApp(
    WS_URL,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)
ws.run_forever()