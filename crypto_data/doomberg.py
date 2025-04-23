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
# GLOBAL IN-MEMORY DATA STRUCT
# -----------------------------
# We'll store raw ticks for candlestick + latest stats for each product.
ticker_data = {
    "BTC-USD": {"ticks": [], "latest": {}},
    "ETH-USD": {"ticks": [], "latest": {}},
}

lock = Lock()

# Kafka settings
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "coinbase-ticker"
KAFKA_GROUP_ID = "aiokafka-group"

# --------------------------------
# ASYNC CONSUMER (BACKGROUND)
# --------------------------------
async def consume_kafka():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: m.decode("utf-8"),
        auto_offset_reset="earliest"
    )
    await consumer.start()
    try:
        print(f"[*] Kafka consumer started on topic '{KAFKA_TOPIC}'...")
        async for msg in consumer:
            data = json.loads(msg.value)
            # Example message structure:
            # {
            #   "channel": "ticker",
            #   "timestamp": "2025-02-18T04:20:19.909066557Z",
            #   "events": [
            #       {"type": "update",
            #        "tickers": [{
            #           "product_id": "BTC-USD", "price": "...", "volume_24_h": "...", etc.
            #        }]
            #       }
            #   ]
            # }
            if data.get("channel") == "ticker":
                timestamp_str = data.get("timestamp", "")
                events = data.get("events", [])
                if events:
                    tickers = events[0].get("tickers", [])
                    if tickers:
                        t = tickers[0]
                        product_id = t.get("product_id")
                        if product_id in ticker_data:
                            price_str = t.get("price")
                            # Update "latest" stats
                            with lock:
                                ticker_data[product_id]["latest"] = t

                                # Append raw tick for candlestick if we have a price
                                if price_str:
                                    price = float(price_str)
                                    try:
                                        dt = datetime.fromisoformat(timestamp_str.replace("Z", ""))
                                    except ValueError:
                                        dt = datetime.utcnow()
                                    ticker_data[product_id]["ticks"].append((dt, price))
                                print(f"[Kafka] {product_id} => {price_str}")
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

# -------------------------------------
# BUILD CANDLESTICK DATA (RESAMPLE)
# -------------------------------------
def build_candlestick(ticks, freq="1Min", lookback_minutes=5):
    """
    Convert list of (timestamp, price) into an OHLC DataFrame,
    showing only the last `lookback_minutes` minutes.
    """
    if not ticks:
        return pd.DataFrame()

    df = pd.DataFrame(ticks, columns=["timestamp", "price"])
    df.set_index("timestamp", inplace=True)

    cutoff = datetime.utcnow() - timedelta(minutes=lookback_minutes)
    df = df[df.index >= cutoff]

    if df.empty:
        return pd.DataFrame()

    # Resample to OHLC
    ohlc = df["price"].resample(freq).agg(["first", "max", "min", "last"])
    ohlc.rename(columns={"first": "open", "max": "high", "min": "low", "last": "close"}, inplace=True)
    return ohlc

# ------------------------
# DASH APP SETUP & STYLES
# ------------------------
app = dash.Dash(__name__)
app.title = "Bloomberg-Style Crypto Dashboard"

# Bloomberg terminal–like styling: black background, neon-green text, monospace font.
APP_STYLE = {
    "backgroundColor": "#000000",  # black
    "color": "#00FF00",            # neon green
    "fontFamily": "monospace",
    "padding": "10px",
}

CARD_STYLE = {
    "backgroundColor": "#111111",
    "border": "1px solid #444444",
    "borderRadius": "5px",
    "padding": "10px",
    "margin": "10px",
    "width": "350px",
}

GRAPH_CONTAINER_STYLE = {
    "backgroundColor": "#111111",
    "border": "1px solid #444444",
    "borderRadius": "5px",
    "padding": "10px",
    "margin": "10px",
    "width": "600px",
}

app.layout = html.Div([
    html.H1("Bloomberg-Style Live Crypto Dashboard (Last 5 Minutes)", style={"textAlign": "center"}),

    html.Div([
        # BTC Stats Card
        html.Div(id="btc-stats", style=CARD_STYLE),
        # ETH Stats Card
        html.Div(id="eth-stats", style=CARD_STYLE),
    ], style={"display": "flex", "flexWrap": "wrap", "justifyContent": "center"}),

    html.Div([
        # BTC Candlestick
        html.Div(dcc.Graph(id="btc-candle"), style=GRAPH_CONTAINER_STYLE),
        # ETH Candlestick
        html.Div(dcc.Graph(id="eth-candle"), style=GRAPH_CONTAINER_STYLE),
    ], style={"display": "flex", "flexWrap": "wrap", "justifyContent": "center"}),

    # Refresh every second
    dcc.Interval(id="interval-component", interval=1000, n_intervals=0)
], style=APP_STYLE)

# ------------------
# BUILD STATS CARD
# ------------------
def build_stats_card(product_id, latest):
    if not latest:
        return html.Div([
            html.H2(f"{product_id}"),
            html.P("No data yet...")
        ])

    price = latest.get("price", "N/A")
    vol_24h = latest.get("volume_24_h", "N/A")
    low_24h = latest.get("low_24_h", "N/A")
    high_24h = latest.get("high_24_h", "N/A")
    low_52w = latest.get("low_52_w", "N/A")
    high_52w = latest.get("high_52_w", "N/A")
    pct_chg_24h = latest.get("price_percent_chg_24_h", "N/A")
    best_bid = latest.get("best_bid", "N/A")
    best_ask = latest.get("best_ask", "N/A")

    return html.Div([
        html.H2(f"{product_id}"),
        html.Ul([
            html.Li(f"Price: {price}"),
            html.Li(f"24h Volume: {vol_24h}"),
            html.Li(f"24h Low: {low_24h}"),
            html.Li(f"24h High: {high_24h}"),
            html.Li(f"52w Low: {low_52w}"),
            html.Li(f"52w High: {high_52w}"),
            html.Li(f"24h % Change: {pct_chg_24h}"),
            html.Li(f"Best Bid: {best_bid}"),
            html.Li(f"Best Ask: {best_ask}")
        ], style={"listStyleType": "none", "paddingLeft": "0"})
    ])

# ------------------
# DASH CALLBACK
# ------------------
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
    with lock:
        # Copy data
        btc_ticks = ticker_data["BTC-USD"]["ticks"][:]
        btc_latest = ticker_data["BTC-USD"]["latest"].copy()

        eth_ticks = ticker_data["ETH-USD"]["ticks"][:]
        eth_latest = ticker_data["ETH-USD"]["latest"].copy()

    # Candlesticks for BTC & ETH (last 5 mins)
    btc_ohlc = build_candlestick(btc_ticks, freq="1Min", lookback_minutes=15)
    eth_ohlc = build_candlestick(eth_ticks, freq="1Min", lookback_minutes=15)

    # Build BTC candlestick figure
    if btc_ohlc.empty:
        fig_btc = go.Figure(layout_title_text="BTC: No Data (last 5 mins)")
    else:
        fig_btc = go.Figure(data=[
            go.Candlestick(
                x=btc_ohlc.index,
                open=btc_ohlc["open"],
                high=btc_ohlc["high"],
                low=btc_ohlc["low"],
                close=btc_ohlc["close"],
                name="BTC",
                increasing_line_color="#00FF00",
                decreasing_line_color="#FF00FF",
            )
        ])
        fig_btc.update_layout(
            title="BTC-USD Candlestick (5 Min)",
            paper_bgcolor="#000000",
            plot_bgcolor="#000000",
            font=dict(color="#00FF00", family="monospace"),
            xaxis_title="Time",
            yaxis_title="Price (USD)"
        )

    # Build ETH candlestick figure
    if eth_ohlc.empty:
        fig_eth = go.Figure(layout_title_text="ETH: No Data (last 5 mins)")
    else:
        fig_eth = go.Figure(data=[
            go.Candlestick(
                x=eth_ohlc.index,
                open=eth_ohlc["open"],
                high=eth_ohlc["high"],
                low=eth_ohlc["low"],
                close=eth_ohlc["close"],
                name="ETH",
                increasing_line_color="#00FF00",
                decreasing_line_color="#FF00FF",
            )
        ])
        fig_eth.update_layout(
            title="ETH-USD Candlestick (5 Min)",
            paper_bgcolor="#000000",
            plot_bgcolor="#000000",
            font=dict(color="#00FF00", family="monospace"),
            xaxis_title="Time",
            yaxis_title="Price (USD)"
        )

    # Build stats for BTC & ETH
    btc_stats = build_stats_card("BTC-USD", btc_latest)
    eth_stats = build_stats_card("ETH-USD", eth_latest)

    return btc_stats, fig_btc, eth_stats, fig_eth

# -----------
# MAIN
# -----------
if __name__ == "__main__":
    # Start the Kafka consumer on a background thread
    consumer_thread = threading.Thread(target=start_consumer_loop, daemon=True)
    consumer_thread.start()

    print("[*] Starting Dash app on http://127.0.0.1:8050")
    app.run_server(debug=True)
