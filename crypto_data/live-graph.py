import asyncio
import json
import threading
from datetime import datetime
from collections import deque
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
MAX_DATA_POINTS = 1000
# Use deque for efficient fixed-size history
market_data = {
    "BTC-USD": {"times": deque(maxlen=MAX_DATA_POINTS), "prices": deque(maxlen=MAX_DATA_POINTS)},
    "ETH-USD": {"times": deque(maxlen=MAX_DATA_POINTS), "prices": deque(maxlen=MAX_DATA_POINTS)}
}

# KAFKA_BOOTSTRAP = "5.tcp.eu.ngrok.io:10707"
KAFKA_BOOTSTRAP = "localhost:19092"
KAFKA_TOPIC = "coinbase-ticker"
KAFKA_GROUP_ID = f"live-graph-consumer-group-{datetime.now().strftime('%Y%m%d%H%M%S')}"

# -----------------------------------
# ASYNC CONSUMER COROUTINE (THREAD)
# -----------------------------------
async def consume_kafka():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: m.decode("utf-8"),
        auto_offset_reset="latest"
    )
    await consumer.start()
    try:
        print(f"[*] Kafka consumer started on topic '{KAFKA_TOPIC}' (Group ID: {KAFKA_GROUP_ID})...")
        async for msg in consumer:
            try:
                data = json.loads(msg.value)
                product_id = None
                price_str = None
                if "product_id" in data and "price" in data:
                    t = data
                    product_id = t.get("product_id")
                    price_str = t.get("price")
                elif data.get("channel") == "ticker":
                     events = data.get("events", [])
                     if events and events[0].get("type") == "update":
                         tickers = events[0].get("tickers", [])
                         if tickers:
                             t = tickers[0]
                             product_id = t.get("product_id")
                             price_str = t.get("price")
                         else: continue
                     else: continue
                else: continue

                if product_id and price_str is not None:
                    price = float(price_str)
                    now = datetime.now()
                    with lock:
                        if product_id in market_data:
                            market_data[product_id]["times"].append(now)
                            market_data[product_id]["prices"].append(price)
            except json.JSONDecodeError:
                print(f"[Error] Skipping invalid JSON: {msg.value[:100]}...")
                continue
            except Exception as e:
                print(f"[Error] Processing message failed: {e}")
                continue

    finally:
        print("[*] Kafka consumer stopping...")
        await consumer.stop()
        print("[*] Kafka consumer stopped.")

def start_consumer_loop():
    """ Run consume_kafka() in an asyncio loop on a background thread. """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(consume_kafka())
    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        print("[*] Consumer task cancelled.")
    finally:
        print("[*] Closing consumer asyncio loop.")
        if task and not task.done():
            task.cancel()
            loop.run_until_complete(asyncio.sleep(0.1))
        loop.close()

# ------------------------
# DASH APP SETUP
# ------------------------
app = dash.Dash(__name__)
app.title = "BTC & ETH Live Plot (Kafka + Dash)"

app.layout = html.Div([
    html.H1("Live Crypto Prices from Kafka"),
    html.Div([
        html.Div([html.H3("BTC-USD Price"), dcc.Graph(id='btc-graph')], style={'width': '49%', 'display': 'inline-block'}),
        html.Div([html.H3("ETH-USD Price"), dcc.Graph(id='eth-graph')], style={'width': '49%', 'display': 'inline-block', 'float': 'right'})
    ]),
    dcc.Interval(
        id='interval-component',
        interval=1000,  # 1 second = 1000 milliseconds
    )
])

@app.callback(
    [Output("btc-graph", "figure"),
     Output("eth-graph", "figure")],
    [Input("interval-component", "n_intervals")]
)
def update_graph_live(n):
    rows = []
    with lock:
        for product_id, data_dict in market_data.items():
            t_list = list(data_dict["times"])
            p_list = list(data_dict["prices"])
            if len(t_list) == len(p_list):
                 for t, p in zip(t_list, p_list):
                     rows.append({
                         "timestamp": t,
                         "price": p,
                         "product_id": product_id
                     })
            else:
                 print(f"[Warning] Mismatched lengths for {product_id}: times={len(t_list)}, prices={len(p_list)}")


    if not rows:
        print("[Callback] No data yet for plotting.")
        empty_layout = {
             "data": [],
             "layout": {
                 "title": "Waiting for Kafka data...",
                 "xaxis": {"title": "Time"},
                 "yaxis": {"title": "Price (USD)"}
             }
         }
        return empty_layout, empty_layout

    df = pd.DataFrame(rows)

    btc_fig = {}
    eth_fig = {}
    error_layout = { "data": [], "layout": { "title": "Error creating plot" }}

    try:
        df_btc = df[df['product_id'] == 'BTC-USD']
        if not df_btc.empty:
            btc_fig = px.line(
                df_btc,
                x="timestamp",
                y="price",
                labels={"timestamp": "Time", "price": "Price (USD)"},
                title=f"BTC-USD - {datetime.now().strftime('%H:%M:%S')}"
            )
        else:
             btc_fig = { "data": [], "layout": { "title": "Waiting for BTC-USD data..." } }
    except Exception as e:
        print(f"[Error] Failed to create BTC figure: {e}")
        btc_fig = error_layout

    try:
        df_eth = df[df['product_id'] == 'ETH-USD']
        if not df_eth.empty:
            eth_fig = px.line(
                df_eth,
                x="timestamp",
                y="price",
                labels={"timestamp": "Time", "price": "Price (USD)"},
                title=f"ETH-USD - {datetime.now().strftime('%H:%M:%S')}"
            )
        else:
             eth_fig = { "data": [], "layout": { "title": "Waiting for ETH-USD data..." } }
    except Exception as e:
        print(f"[Error] Failed to create ETH figure: {e}")
        eth_fig = error_layout

    return btc_fig, eth_fig


# -----------
# MAIN
# -----------
if __name__ == '__main__':
    print("[*] Starting Kafka consumer thread...")
    consumer_thread = threading.Thread(target=start_consumer_loop, daemon=True)
    consumer_thread.start()

    print("[*] Starting Dash app on http://127.0.0.1:8050")
    app.run(debug=True, use_reloader=False)
