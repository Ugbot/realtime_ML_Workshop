import asyncio
import json
import threading
from datetime import datetime, timedelta

import pandas as pd
from aiokafka import AIOKafkaConsumer

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from threading import Lock

# -----------------------------
# GLOBAL IN-MEMORY DATA STRUCTS
# -----------------------------
# We'll track both "raw ticks" for candlestick aggregation,
# and "latest stats" from the message for each product.
ticker_data = {
    "BTC-USD": {
        "ticks": [],  # list of (timestamp, price) for candlestick
        "latest": {}  # holds the latest fields like volume_24_h, best_bid, etc.
    },
    "ETH-USD": {
        "ticks": [],
        "latest": {}
    }
}

# A lock to protect concurrent access
lock = Lock()

# Kafka config
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "coinbase-ticker"
KAFKA_GROUP_ID = "aiokafka-group"


# -------------
# KAFKA CONSUMER
# -------------
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
            if data.get("channel") == "ticker":
                timestamp_str = data.get("timestamp", "")
                events = data.get("events", [])
                if events:
                    tickers = events[0].get("tickers", [])
                    if tickers:
                        t = tickers[0]
                        product_id = t.get("product_id")  # BTC-USD or ETH-USD
                        if product_id in ticker_data:
                            # Gather relevant fields
                            price_str = t.get("price")
                            if price_str:
                                price = float(price_str)
                            else:
                                price = None

                            # We'll store the entire ticker object in "latest"
                            with lock:
                                # 1) Update "latest" info
                                ticker_data[product_id]["latest"] = t

                                # 2) Append a raw tick for candlesticks
                                if price is not None:
                                    # Use message-level or fallback to "now"
                                    if timestamp_str:
                                        try:
                                            # If there's a 'Z' at the end, or fractional seconds, parse carefully
                                            # As a fallback, we can parse or just use now()
                                            dt = datetime.fromisoformat(timestamp_str.replace("Z", ""))
                                        except ValueError:
                                            dt = datetime.utcnow()
                                    else:
                                        dt = datetime.utcnow()

                                    ticker_data[product_id]["ticks"].append((dt, price))

                                print(f"[Kafka] {product_id} => {price}")
    finally:
        await consumer.stop()


def start_consumer_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(consume_kafka())
    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()


# -------------
# RESAMPLE LOGIC
# -------------
def build_candlestick(ticks, freq="1Min", lookback_minutes=5):
    """
    Resample raw ticks into OHLC for candlestick chart.
    `freq` = "1Min" by default, showing each candle as 1 minute.
    Only shows last `lookback_minutes` minutes of data.
    """
    if not ticks:
        return pd.DataFrame()

    df = pd.DataFrame(ticks, columns=["timestamp", "price"])
    df.set_index("timestamp", inplace=True)
    cutoff = datetime.utcnow() - timedelta(minutes=lookback_minutes)
    df = df[df.index >= cutoff]

    # Resample to OHLC
    ohlc = df["price"].resample(freq).agg(["first", "max", "min", "last"])
    ohlc.rename(columns={"first": "open", "max": "high", "min": "low", "last": "close"}, inplace=True)
    return ohlc


# -----------------------------
# DASH APP & LAYOUT
# -----------------------------
app = dash.Dash(__name__)
app.title = "Cool Crypto Dashboard"

app.layout = html.Div([
    html.H1("Live Crypto Dashboard (Last 5 Minutes)"),

    # A row for stats: BTC vs. ETH
    html.Div([
        html.Div(id="btc-stats", className="three columns", style={"padding": "10px"}),
        html.Div(id="eth-stats", className="three columns", style={"padding": "10px"}),
    ], className="row", style={"display": "flex"}),

    # A row for candlestick graphs
    html.Div([
        html.Div(dcc.Graph(id="btc-candle"), className="six columns"),
        html.Div(dcc.Graph(id="eth-candle"), className="six columns"),
    ], className="row", style={"display": "flex"}),

    # Interval to update every second
    dcc.Interval(id="interval-component", interval=1000, n_intervals=0)
], style={"max-width": "90%", "margin": "auto"})


# -----------------------------
# DASH CALLBACK
# -----------------------------
@app.callback(
    [
        Output("btc-stats", "children"),
        Output("btc-candle", "figure"),
        Output("eth-stats", "children"),
        Output("eth-candle", "figure"),
    ],
    [Input("interval-component", "n_intervals")]
)
def update_dashboard(n):
    """
    Called once per second:
    1) Build a candlestick chart for BTC & ETH (last 5 minutes, 1-minute intervals)
    2) Display stats from 'latest' ticker data for each
    """
    with lock:
        # Copy data for BTC
        btc_ticks = ticker_data["BTC-USD"]["ticks"][:]
        btc_latest = ticker_data["BTC-USD"]["latest"].copy()

        # Copy data for ETH
        eth_ticks = ticker_data["ETH-USD"]["ticks"][:]
        eth_latest = ticker_data["ETH-USD"]["latest"].copy()

    # Build candlestick for BTC
    btc_ohlc = build_candlestick(btc_ticks, freq="1Min", lookback_minutes=5)
    if btc_ohlc.empty:
        fig_btc = go.Figure(layout_title_text="No BTC data (last 5 min)")
    else:
        fig_btc = go.Figure(data=[
            go.Candlestick(
                x=btc_ohlc.index,
                open=btc_ohlc["open"],
                high=btc_ohlc["high"],
                low=btc_ohlc["low"],
                close=btc_ohlc["close"],
                name="BTC",
                increasing_line_color="green",
                decreasing_line_color="red"
            )
        ])
        fig_btc.update_layout(
            title="BTC-USD Candlestick (5 Min)",
            xaxis_title="Time",
            yaxis_title="Price (USD)"
        )

    # Build candlestick for ETH
    eth_ohlc = build_candlestick(eth_ticks, freq="1Min", lookback_minutes=5)
    if eth_ohlc.empty:
        fig_eth = go.Figure(layout_title_text="No ETH data (last 5 min)")
    else:
        fig_eth = go.Figure(data=[
            go.Candlestick(
                x=eth_ohlc.index,
                open=eth_ohlc["open"],
                high=eth_ohlc["high"],
                low=eth_ohlc["low"],
                close=eth_ohlc["close"],
                name="ETH",
                increasing_line_color="green",
                decreasing_line_color="red"
            )
        ])
        fig_eth.update_layout(
            title="ETH-USD Candlestick (5 Min)",
            xaxis_title="Time",
            yaxis_title="Price (USD)"
        )

    # Build stats "cards" for BTC & ETH (HTML)
    btc_stats_card = build_stats_card("BTC-USD", btc_latest)
    eth_stats_card = build_stats_card("ETH-USD", eth_latest)

    return btc_stats_card, fig_btc, eth_stats_card, fig_eth


def build_stats_card(product_id, latest_ticker):
    """
    Return an HTML component displaying the latest fields for a given product.
    We read fields like price, volume_24_h, best_bid, etc. from `latest_ticker`.
    """
    if not latest_ticker:
        return html.Div([
            html.H3(f"{product_id} Stats"),
            html.P("No data yet...")
        ])

    # Extract fields safely
    price = latest_ticker.get("price", "N/A")
    vol_24h = latest_ticker.get("volume_24_h", "N/A")
    low_24h = latest_ticker.get("low_24_h", "N/A")
    high_24h = latest_ticker.get("high_24_h", "N/A")
    low_52w = latest_ticker.get("low_52_w", "N/A")
    high_52w = latest_ticker.get("high_52_w", "N/A")
    pct_change_24h = latest_ticker.get("price_percent_chg_24_h", "N/A")
    best_bid = latest_ticker.get("best_bid", "N/A")
    best_ask = latest_ticker.get("best_ask", "N/A")

    return html.Div([
        html.H3(f"{product_id} Stats"),
        html.Ul([
            html.Li(f"Price: {price}"),
            html.Li(f"24h Volume: {vol_24h}"),
            html.Li(f"24h Low: {low_24h}"),
            html.Li(f"24h High: {high_24h}"),
            html.Li(f"52w Low: {low_52w}"),
            html.Li(f"52w High: {high_52w}"),
            html.Li(f"24h % Change: {pct_change_24h}"),
            html.Li(f"Best Bid: {best_bid}"),
            html.Li(f"Best Ask: {best_ask}")
        ], style={"listStyleType": "none", "paddingLeft": "0"})
    ], style={
        "border": "1px solid #ccc",
        "borderRadius": "5px",
        "padding": "10px",
        "margin": "10px"
    })


# -----------
# MAIN
# -----------
if __name__ == "__main__":
    # Start the Kafka consumer in a background thread
    consumer_thread = threading.Thread(target=start_consumer_loop, daemon=True)
    consumer_thread.start()

    # Run Dash
    print("[*] Starting Dash app on http://127.0.0.1:8050")
    app.run_server(debug=True)
