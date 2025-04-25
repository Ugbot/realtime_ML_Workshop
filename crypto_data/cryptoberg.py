import asyncio
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

import pandas as pd
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

import dash
from aiosignal import Signal
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from threading import Lock
import numpy as np

# --------------------------------------------------
# GLOBAL STATE
# --------------------------------------------------
# We'll create entries on the fly for each product_id that appears.
ticker_data = {}  # e.g. { "BTC-USD": {"ticks": [...], "latest": {...}}, ... }

lock = Lock()

# Kafka config
KAFKA_BOOTSTRAP = "localhost:19092"
KAFKA_TOPIC = "coinbase-ticker"
KAFKA_GROUP_ID = "aiokafka-group"
KAFKA_SIGNALS_TOPIC = "trading-signals"
SIGNAL_TYPES = ["CALL_BUY", "PUT_BUY", "CALL_SELL", "PUT_SELL"]

# ------------------------------
# ASYNC KAFKA CONSUMER
# ------------------------------
async def consume_kafka():
    # Create producer for signals
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    await producer.start()
    
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: m.decode("utf-8"),
        # auto_offset_reset= "earliest"
    )
    await consumer.start()
    try:
        print(f"[*] Kafka consumer started on topic '{KAFKA_TOPIC}'...")
        async for msg in consumer:
            data = json.loads(msg.value)
            # {
            #   "channel": "ticker",
            #   "timestamp": "...",
            #   "events": [{"tickers": [{"product_id": "...", "price": "...", ...}]}]
            # }
            if data.get("channel") == "ticker":
                timestamp_str = data.get("timestamp", "")
                events = data.get("events", [])
                if events:
                    tickers = events[0].get("tickers", [])
                    if tickers:
                        t = tickers[0]
                        product_id = t.get("product_id")
                        if product_id:
                            price_str = t.get("price")
                            with lock:
                                # If new product_id, add a structure
                                if product_id not in ticker_data:
                                    ticker_data[product_id] = {
                                        "ticks": [],
                                        "latest": {}
                                    }
                                # Update the "latest" data
                                ticker_data[product_id]["latest"] = t

                                # Store raw tick
                                if price_str:
                                    try:
                                        price = float(price_str)
                                        try:
                                            dt = datetime.fromisoformat(timestamp_str.replace("Z", ""))
                                        except ValueError:
                                            dt = datetime.utcnow()
                                        ticker_data[product_id]["ticks"].append((dt, price))
                                    except ValueError:
                                        pass
                                print(f"[Kafka] {product_id} => {price_str}")

                            # Generate trading signals
                            signal = analyze_for_signals(
                                product_id, 
                                ticker_data[product_id]["ticks"]
                            )
                            if signal:
                                # Publish signal to Kafka
                                await producer.send_and_wait(
                                    KAFKA_SIGNALS_TOPIC,
                                    signal.to_json()
                                )
                                print(f"[Signal] {product_id} => {signal.signal_type}")
    finally:
        await consumer.stop()
        await producer.stop()

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

# ---------------------------------------------------------
# HELPER: BUILD STATS CARD
# ---------------------------------------------------------
def build_stats_card(product_id, latest):
    if not latest:
        return html.Div([
            html.H2(product_id),
            html.P("No data yet...")
        ])

    # Extract some fields
    price = latest.get("price", "N/A")
    vol_24h = latest.get("volume_24_h", "N/A")
    low_24h = latest.get("low_24_h", "N/A")
    high_24h = latest.get("high_24_h", "N/A")
    low_52w = latest.get("low_52_w", "N/A")
    high_52w = latest.get("high_52_w", "N/A")
    pct_chg_24h = latest.get("price_percent_chg_24_h", "N/A")
    best_bid = latest.get("best_bid", "N/A")
    best_ask = latest.get("best_ask", "N/A")

    card_style = {
        "backgroundColor": "#111111",
        "border": "1px solid #444444",
        "borderRadius": "5px",
        "padding": "10px",
        "margin": "10px",
        "width": "320px"
    }

    # Add signals section to the card
    with lock:
        ticks = ticker_data.get(product_id, {}).get("ticks", [])
        signal = analyze_for_signals(product_id, ticks)
    
    signal_color = "#FFFF00" if signal else "#666666"
    signal_text = f"Signal: {signal.signal_type}" if signal else "No active signals"
    
    return html.Div([
        html.H2(f"{product_id}"),
        html.Ul([
            html.Li(f"Price: {price}"),
            html.Li(f"24h Vol: {vol_24h}"),
            html.Li(f"24h Low: {low_24h}"),
            html.Li(f"24h High: {high_24h}"),
            html.Li(f"52w Low: {low_52w}"),
            html.Li(f"52w High: {high_52w}"),
            html.Li(f"24h %: {pct_chg_24h}"),
            html.Li(f"Best Bid: {best_bid}"),
            html.Li(f"Best Ask: {best_ask}")
        ], style={"listStyleType": "none", "paddingLeft": "0"}),
        html.Div([
            html.Hr(style={"borderColor": "#444444"}),
            html.P(signal_text, style={"color": signal_color}),
        ])
    ], style=card_style)

# ---------------------------------------------------------
# HELPER: BUILD CANDLE OHLC
# ---------------------------------------------------------
def build_ohlc(ticks, freq="1Min", lookback_minutes=5):
    """
    Convert (timestamp, price) ticks into an OHLC DataFrame,
    restricted to the last `lookback_minutes`.
    freq = "1Min" => 1-minute bars
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
    ohlc.rename(columns={
        "first": "open",
        "max": "high",
        "min": "low",
        "last": "close"
    }, inplace=True)

    return ohlc

# ---------------------------------------------------------
# HELPER: BUILD RAW LINE DF
# ---------------------------------------------------------
def build_line_data(ticks, lookback_minutes=5):
    """
    Return a DataFrame of raw ticks (time vs. price), filtered to last `lookback_minutes`.
    No resampling, so we see every data point.
    """
    if not ticks:
        return pd.DataFrame()

    df = pd.DataFrame(ticks, columns=["timestamp", "price"])
    df.set_index("timestamp", inplace=True)
    cutoff = datetime.utcnow() - timedelta(minutes=lookback_minutes)
    df = df[df.index >= cutoff].sort_index()
    return df

# Add these new functions before the consume_kafka function
def calculate_bollinger_bands(prices: List[float], window: int = 20, 
                            num_std: float = 2.0) -> Tuple[float, float, float]:
    """Calculate Bollinger Bands for a series of prices."""
    prices_arr = np.array(prices)
    sma = np.mean(prices_arr[-window:])
    std = np.std(prices_arr[-window:])
    upper_band = sma + (std * num_std)
    lower_band = sma - (std * num_std)
    return upper_band, sma, lower_band

def analyze_for_signals(product_id: str, ticks: List[Tuple[datetime, float]]) -> Optional[Signal]:
    """Analyze price data and generate trading signals."""
    if len(ticks) < 20:  # Need at least 20 points for analysis
        return None
    
    # Get latest price and timestamp
    latest_dt, latest_price = ticks[-1]
    prices = [price for _, price in ticks]
    
    # Calculate Bollinger Bands
    upper_band, sma, lower_band = calculate_bollinger_bands(prices)
    
    # Calculate price momentum (simple)
    momentum = prices[-1] - prices[-5] if len(prices) >= 5 else 0
    
    # Generate signals based on analysis
    signal = None
    metadata = {
        "upper_band": upper_band,
        "sma": sma,
        "lower_band": lower_band,
        "momentum": momentum
    }
    
    if latest_price <= lower_band and momentum > 0:
        # Price at lower band with positive momentum - potential call buy
        signal = Signal(
            product_id=product_id,
            signal_type="CALL_BUY",
            price=latest_price,
            timestamp=latest_dt,
            confidence=0.7,
            metadata=metadata
        )
    elif latest_price >= upper_band and momentum < 0:
        # Price at upper band with negative momentum - potential put buy
        signal = Signal(
            product_id=product_id,
            signal_type="PUT_BUY",
            price=latest_price,
            timestamp=latest_dt,
            confidence=0.7,
            metadata=metadata
        )
    
    return signal

# Add these new types after the constants
class Signal:
    def __init__(self, product_id: str, signal_type: str, price: float, 
                 timestamp: datetime, confidence: float, metadata: Dict):
        self.product_id = product_id
        self.signal_type = signal_type
        self.price = price
        self.timestamp = timestamp
        self.confidence = confidence
        self.metadata = metadata

    def to_json(self) -> Dict:
        return {
            "product_id": self.product_id,
            "signal_type": self.signal_type,
            "price": self.price,
            "timestamp": self.timestamp.isoformat(),
            "confidence": self.confidence,
            "metadata": self.metadata
        }

# ---------------------------------------------------------
# DASH APP SETUP & LAYOUT
# ---------------------------------------------------------
app = dash.Dash(__name__)
app.title = "Bloomberg-Style: Dynamic Candles + Raw Line"

APP_STYLE = {
    "backgroundColor": "#000000",
    "color": "#00FF00",
    "fontFamily": "monospace",
    "padding": "10px",
}

app.layout = html.Div([
    html.H1("Bloomberg-Style Crypto Dashboard (Candles + Raw Line)", style={"textAlign": "center"}),

    html.Div([
        html.H3("Select Time Range (Minutes): "),
        dcc.RadioItems(
            id="time-range",
            options=[
                {"label": "1 min", "value": 1},
                {"label": "5 mins", "value": 5},
                {"label": "30 mins", "value": 30},
            ],
            value=5,  # default
            inline=True,
            style={"marginBottom": "20px"}
        )
    ], style={"textAlign": "center"}),

    # We'll build dynamic content (cards + charts) in this container
    html.Div(id="main-content"),

    # Refresh every 2 seconds
    dcc.Interval(id="interval-component", interval=2000, n_intervals=0)
], style=APP_STYLE)


# ---------------------------------------------------------
# DASH CALLBACK
# ---------------------------------------------------------
@app.callback(
    Output("main-content", "children"),
    [
        Input("interval-component", "n_intervals"),
        Input("time-range", "value"),
    ]
)
def update_dashboard(n, selected_range):
    """
    Called every 2s or whenever the user changes the time range radio.
    For each discovered symbol, build:
      - stats card
      - candlestick chart (resampled 1Min data)
      - line chart (ALL raw points, just time-filtered)
    """
    with lock:
        product_ids = sorted(ticker_data.keys())

    rows = []
    for product_id in product_ids:
        with lock:
            ticks = ticker_data[product_id]["ticks"][:]
            latest = ticker_data[product_id]["latest"].copy()

        # Stats card
        stats_card = build_stats_card(product_id, latest)

        # Candlestick data: 1-minute OHLC
        ohlc = build_ohlc(ticks, freq="1Min", lookback_minutes=selected_range)
        # Raw line data: no resample, all points
        line_df = build_line_data(ticks, lookback_minutes=selected_range)

        # ---- Candle Figure
        if ohlc.empty:
            candle_fig = go.Figure(layout_title_text=f"{product_id}: No Candle Data (last {selected_range} mins)")
        else:
            candle_fig = go.Figure(data=[
                go.Candlestick(
                    x=ohlc.index,
                    open=ohlc["open"],
                    high=ohlc["high"],
                    low=ohlc["low"],
                    close=ohlc["close"],
                    name=f"{product_id}",
                    increasing_line_color="#00FF00",
                    decreasing_line_color="#FF00FF",
                )
            ])
            candle_fig.update_layout(
                title=f"{product_id} Candlestick ({selected_range} Min)",
                paper_bgcolor="#000000",
                plot_bgcolor="#000000",
                font=dict(color="#00FF00", family="monospace"),
                xaxis_title="Time",
                yaxis_title="Price (USD)"
            )

        # ---- Line Figure (all points)
        if line_df.empty:
            line_fig = go.Figure(layout_title_text=f"{product_id}: No Raw Data (last {selected_range} mins)")
        else:
            line_fig = go.Figure(data=[
                go.Scatter(
                    x=line_df.index,
                    y=line_df["price"],
                    mode="lines+markers",
                    line=dict(color="#00FFFF", width=2),
                    name=f"{product_id} (raw line)"
                )
            ])
            line_fig.update_layout(
                title=f"{product_id} Line Chart (ALL points, {selected_range} Min)",
                paper_bgcolor="#000000",
                plot_bgcolor="#000000",
                font=dict(color="#00FF00", family="monospace"),
                xaxis_title="Time",
                yaxis_title="Price (USD)"
            )

        # Layout for each product: stats + candle + line
        row_style = {
            "display": "flex",
            "flexWrap": "wrap",
            "justifyContent": "flex-start",
            "alignItems": "flex-start",
            "backgroundColor": "#000000",
            "marginBottom": "60px",
            "borderBottom": "1px solid #333333"
        }

        chart_style = {
            "backgroundColor": "#111111",
            "border": "1px solid #444444",
            "borderRadius": "5px",
            "padding": "10px",
            "margin": "10px",
            "width": "550px"
        }

        row = html.Div([
            stats_card,
            html.Div(dcc.Graph(figure=candle_fig), style=chart_style),
            html.Div(dcc.Graph(figure=line_fig), style=chart_style),
        ], style=row_style)

        rows.append(row)

    return rows

# -----------
# MAIN
# -----------
if __name__ == "__main__":
    consumer_thread = threading.Thread(target=start_consumer_loop, daemon=True)
    consumer_thread.start()

    print("[*] Starting Dash app on http://127.0.0.1:8055")
    app.run_server(debug=True, port=8055)
