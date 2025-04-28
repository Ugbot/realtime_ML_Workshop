import asyncio
import json
import threading
from datetime import datetime
from collections import deque
from aiokafka import AIOKafkaConsumer
from typing import Dict, Deque, Any, List

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd

from threading import Lock

# ----------------------------
# GLOBALS - SHARED STATE
# ----------------------------
lock: Lock = Lock()
MAX_DATA_POINTS: int = 1000
# Use deque for efficient fixed-size history for RSI data
rsi_data: Dict[str, Dict[str, Deque[Any]]] = {
    "ETH-USD": {"times": deque(maxlen=MAX_DATA_POINTS), "rsi14": deque(maxlen=MAX_DATA_POINTS)}
}

# KAFKA_BOOTSTRAP = "5.tcp.eu.ngrok.io:10707"  # Example Ngrok
KAFKA_BOOTSTRAP: str = "localhost:19092"
KAFKA_TOPIC: str = "eth-rsi"  # Consume from the RSI topic
KAFKA_GROUP_ID: str = f"rsi-live-graph-consumer-group-{datetime.now().strftime('%Y%m%d%H%M%S')}"

# -----------------------------------
# ASYNC CONSUMER COROUTINE (THREAD)
# -----------------------------------
async def consume_kafka() -> None:
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: m.decode("utf-8"),
        auto_offset_reset="latest" # Start from the latest messages
    )
    await consumer.start()
    try:
        print(f"[*] Kafka consumer started on topic '{KAFKA_TOPIC}' (Group ID: {KAFKA_GROUP_ID})...")
        async for msg in consumer:
            try:
                data = json.loads(msg.value)
                product_id: str | None = data.get("product_id")
                rsi_value_str: str | None = data.get("rsi14") # Get rsi14 value

                if product_id == "ETH-USD" and rsi_value_str is not None:
                    rsi_value: float = float(rsi_value_str)
                    now: datetime = datetime.now()
                    with lock:
                        if product_id in rsi_data:
                            rsi_data[product_id]["times"].append(now)
                            rsi_data[product_id]["rsi14"].append(rsi_value)
            except json.JSONDecodeError:
                print(f"[Error] Skipping invalid JSON: {msg.value[:100]}...")
                continue
            except (ValueError, TypeError) as e:
                 print(f"[Error] Could not parse RSI value: {e} - Data: {msg.value}")
                 continue
            except Exception as e:
                print(f"[Error] Processing message failed: {e}")
                continue

    finally:
        print("[*] Kafka consumer stopping...")
        await consumer.stop()
        print("[*] Kafka consumer stopped.")

def start_consumer_loop() -> None:
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
            # Give cancellation a moment to propagate
            loop.run_until_complete(asyncio.sleep(0.1))
        loop.close()

# ------------------------
# DASH APP SETUP
# ------------------------
app = dash.Dash(__name__)
app.title = "ETH RSI Live Plot (Kafka + Dash)"

app.layout = html.Div([
    html.H1("Live Ethereum RSI from Kafka"),
    html.Div([
        html.Div([html.H3("ETH-USD RSI (14-period)"), dcc.Graph(id='eth-rsi-graph')], style={'width': '98%', 'display': 'inline-block'})
    ]),
    dcc.Interval(
        id='interval-component',
        interval=1000,  # 1 second = 1000 milliseconds
        n_intervals=0
    )
])

@app.callback(
    Output("eth-rsi-graph", "figure"),
    Input("interval-component", "n_intervals")
)
def update_graph_live(n: int) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    with lock:
        # Only ETH-USD data is expected here
        if "ETH-USD" in rsi_data:
            data_dict = rsi_data["ETH-USD"]
            t_list: List[datetime] = list(data_dict["times"])
            r_list: List[float] = list(data_dict["rsi14"])
            if len(t_list) == len(r_list):
                 for t, r in zip(t_list, r_list):
                     rows.append({
                         "timestamp": t,
                         "rsi14": r,
                         "product_id": "ETH-USD" # Explicitly add product_id
                     })
            else:
                 print(f"[Warning] Mismatched lengths for ETH-USD RSI: times={len(t_list)}, rsi14={len(r_list)}")


    if not rows:
        print("[Callback] No RSI data yet for plotting.")
        empty_layout: Dict[str, Any] = {
             "data": [],
             "layout": {
                 "title": "Waiting for Kafka RSI data...",
                 "xaxis": {"title": "Time"},
                 "yaxis": {"title": "RSI (14-period)"}
             }
         }
        return empty_layout

    df = pd.DataFrame(rows)
    fig: Dict[str, Any] = {}
    error_layout: Dict[str, Any] = { "data": [], "layout": { "title": "Error creating RSI plot" }}

    try:
        if not df.empty:
            fig = px.line(
                df,
                x="timestamp",
                y="rsi14", # Plot the rsi14 value
                labels={"timestamp": "Time", "rsi14": "RSI (14-period)"},
                title=f"ETH-USD RSI - {datetime.now().strftime('%H:%M:%S')}"
            )
            # Add horizontal lines for overbought/oversold levels
            fig.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="Overbought (70)", annotation_position="bottom right")
            fig.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="Oversold (30)", annotation_position="bottom right")
            fig.update_yaxes(range=[0, 100]) # RSI is typically 0-100
        else:
             # Use a more informative title when waiting
             fig = { "data": [], "layout": { "title": "Waiting for ETH-USD RSI data...", "yaxis": {"range": [0, 100]}} }
    except Exception as e:
        print(f"[Error] Failed to create ETH RSI figure: {e}")
        fig = error_layout

    return fig


# -----------
# MAIN
# -----------
if __name__ == '__main__':
    print("[*] Starting Kafka RSI consumer thread...")
    consumer_thread = threading.Thread(target=start_consumer_loop, daemon=True)
    consumer_thread.start()

    print("[*] Starting Dash app on http://127.0.0.1:8051") # Use a different port
    # use_reloader=False is important when running consumer in a thread
    app.run(debug=True, use_reloader=False, port=8051) 