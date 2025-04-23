import asyncio
import json
import threading
from datetime import datetime, timedelta
from aiokafka import AIOKafkaConsumer

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from threading import Lock

# ----------------------------------------------------
# GLOBAL STATE - in-memory data keyed by product_id
# ----------------------------------------------------
lock = Lock()
market_data = {
    "BTC-USD": {"times": [], "prices": []},
    "ETH-USD": {"times": [], "prices": []}
}

# Kafka connection details
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "coinbase-ticker"
KAFKA_GROUP_ID = "aiokafka-group"

# ----------------------------------------------------
# ASYNC KAFKA CONSUMER
# ----------------------------------------------------
async def consume_kafka():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: m.decode("utf-8"),
        # auto_offset_reset="earliest" or "latest"
    )
    await consumer.start()
    try:
        print(f"[*] Kafka consumer started on topic '{KAFKA_TOPIC}'...")
        async for msg in consumer:
            data = json.loads(msg.value)
            # Expected structure from Coinbase Advanced Trade WS:
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
                            timestamp = datetime.now()
                            with lock:
                                # Store the new data point
                                if product_id in market_data:
                                    market_data[product_id]["times"].append(timestamp)
                                    market_data[product_id]["prices"].append(price)
                            print(f"[Kafka] {product_id} price: {price}")
    finally:
        await consumer.stop()

def start_consumer_loop():
    """Run consume_kafka() in an asyncio loop on a background thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(consume_kafka())
    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()

# ----------------------------------------------------
# DASH APP
# ----------------------------------------------------
app = dash.Dash(__name__)
app.title = "Crypto Live Price - BTC & ETH"
app.layout = html.Div([
    html.H1("BTC & ETH Live Price (Last 1 Hour)"),

    # Graph for BTC
    html.Div([
        html.H2("BTC-USD"),
        dcc.Graph(id="btc-graph"),
    ], style={"margin-bottom": "50px"}),

    # Graph for ETH
    html.Div([
        html.H2("ETH-USD"),
        dcc.Graph(id="eth-graph"),
    ]),

    # Interval update every second
    dcc.Interval(
        id="interval-component",
        interval=1000,  # 1 second
        n_intervals=0
    )
])

@app.callback(
    [Output("btc-graph", "figure"), Output("eth-graph", "figure")],
    [Input("interval-component", "n_intervals")]
)
def update_graph_live(n):
    """
    Pull data from our global state (market_data) for each product.
    Filter to show only the last hour of data, then build 2 separate figures.
    """
    # 1) Copy data under lock to avoid race conditions
    with lock:
        # We keep all data in memory but we'll slice it to the last hour in the figure
        btc_times = market_data["BTC-USD"]["times"][:]
        btc_prices = market_data["BTC-USD"]["prices"][:]
        eth_times = market_data["ETH-USD"]["times"][:]
        eth_prices = market_data["ETH-USD"]["prices"][:]

    # 2) Filter to the last hour
    one_hour_ago = datetime.now() - timedelta(minutes=2)

    # Build DataFrame for BTC
    btc_df = pd.DataFrame({"time": btc_times, "price": btc_prices})
    btc_df = btc_df[btc_df["time"] > one_hour_ago]

    # Build DataFrame for ETH
    eth_df = pd.DataFrame({"time": eth_times, "price": eth_prices})
    eth_df = eth_df[eth_df["time"] > one_hour_ago]

    # 3) Create the BTC figure
    if len(btc_df) == 0:
        fig_btc = px.line(title="No BTC data in last hour")
    else:
        fig_btc = px.line(
            btc_df,
            x="time",
            y="price",
            title="BTC-USD (Last 1 Hour)",
            labels={"time": "Time", "price": "Price (USD)"}
        )

    # 4) Create the ETH figure
    if len(eth_df) == 0:
        fig_eth = px.line(title="No ETH data in last hour")
    else:
        fig_eth = px.line(
            eth_df,
            x="time",
            y="price",
            title="ETH-USD (Last 1 Hour)",
            labels={"time": "Time", "price": "Price (USD)"}
        )

    return fig_btc, fig_eth

# -----------
# MAIN
# -----------
if __name__ == "__main__":
    # 1) Start the Kafka consumer in a background thread
    consumer_thread = threading.Thread(target=start_consumer_loop, daemon=True)
    consumer_thread.start()

    # 2) Start the Dash server
    print("[*] Starting Dash app on http://127.0.0.1:8050")
    app.run_server(debug=True)
