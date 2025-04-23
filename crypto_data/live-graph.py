import asyncio
import json
import threading
from datetime import datetime
from aiokafka import AIOKafkaConsumer

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd

from threading import Lock

# ----------------------------
# GLOBALS - SHARED STATE
# ----------------------------
lock = Lock()
# We'll keep a dictionary keyed by product_id => { "times": [], "prices": [] }
market_data = {
    "BTC-USD": {"times": [], "prices": []},
    "ETH-USD": {"times": [], "prices": []}
}

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "coinbase-ticker"
KAFKA_GROUP_ID = "aiokafka-group"

# -----------------------------------
# ASYNC CONSUMER COROUTINE (THREAD)
# -----------------------------------
async def consume_kafka():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: m.decode("utf-8"),
        # auto_offset_reset="latest" or "earliest"
    )
    await consumer.start()
    try:
        print(f"[*] Kafka consumer started on topic '{KAFKA_TOPIC}'...")
        async for msg in consumer:
            data = json.loads(msg.value)
            # The expected structure from the WebSocket is:
            # {
            #   "channel": "ticker",
            #   "events": [
            #       {
            #           "type": "update",
            #           "tickers": [
            #             {"product_id": "BTC-USD", "price": "XXXXX", ...}
            #           ]
            #       }
            #   ]
            # }
            channel = data.get("channel")
            if channel == "ticker":
                events = data.get("events", [])
                if events:
                    tickers = events[0].get("tickers", [])
                    if tickers:
                        t = tickers[0]
                        product_id = t.get("product_id")  # e.g. "BTC-USD" or "ETH-USD"
                        price_str = t.get("price")
                        if product_id and price_str:
                            price = float(price_str)
                            with lock:
                                # Append timestamp & price to the correct product
                                if product_id in market_data:
                                    market_data[product_id]["times"].append(datetime.now())
                                    market_data[product_id]["prices"].append(price)
                            print(f"[Kafka] {product_id} price: {price}")
    finally:
        await consumer.stop()

def start_consumer_loop():
    """ Run consume_kafka() in an asyncio loop on a background thread. """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(consume_kafka())
    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()

# ------------------------
# DASH APP SETUP
# ------------------------
app = dash.Dash(__name__)
app.title = "BTC & ETH Live Plot (Kafka + Dash)"

app.layout = html.Div([
    html.H1("BTC & ETH Live Price from Kafka"),
    dcc.Graph(id='live-graph'),
    dcc.Interval(
        id='interval-component',
        interval=1000,  # 1 second
        n_intervals=0
    )
])

@app.callback(
    Output("live-graph", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_graph_live(n):
    # We'll build a single DataFrame that has columns: ["timestamp", "price", "product_id"]
    rows = []
    with lock:
        for product_id, data_dict in market_data.items():
            t_list = data_dict["times"]
            p_list = data_dict["prices"]
            for t, p in zip(t_list, p_list):
                rows.append({
                    "timestamp": t,
                    "price": p,
                    "product_id": product_id
                })

    if not rows:
        # If there's no data yet, return an empty figure
        return px.line(title="No data yet")

    df = pd.DataFrame(rows)
    # Now we can plot both BTC and ETH on one figure
    # color="product_id" means separate lines by product_id
    fig = px.line(
        df,
        x="timestamp",
        y="price",
        color="product_id",
        labels={"timestamp": "Time", "price": "Price (USD)", "product_id": "Product"},
        title="BTC & ETH Price (Live from Kafka)"
    )
    return fig

# -----------
# MAIN
# -----------
if __name__ == '__main__':
    # 1) Start consumer in a background thread
    consumer_thread = threading.Thread(target=start_consumer_loop, daemon=True)
    consumer_thread.start()

    # 2) Run the Dash app
    print("[*] Starting Dash app on http://127.0.0.1:8050")
    app.run_server(debug=True)
