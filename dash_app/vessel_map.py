#!/usr/bin/env python3
"""
Real-time vessel tracking dashboard using Dash and Plotly.

This application:
1. Consumes vessel position data from Kafka topic 'vessel_tracks'
2. Displays vessel positions on an interactive map
3. Shows vessel trajectories over time
4. Updates in real-time as new position data arrives
5. Supports filtering and clustering of vessels

Requirements:
- dash
- plotly
- aiokafka
- pandas

Usage:
    python vessel_map.py
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import dash
from dash import dcc, html, Input, Output, callback, State
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from aiokafka import AIOKafkaConsumer
import logging
from threading import Lock

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# GLOBAL STATE
# --------------------------------------------------
# Store vessel positions with a time-based window
MAX_POSITIONS_PER_VESSEL = 100  # Keep last 100 positions per vessel
MAX_VESSELS = 1000  # Limit total number of vessels to track
vessel_positions: Dict[str, deque] = defaultdict(lambda: deque(maxlen=MAX_POSITIONS_PER_VESSEL))
vessel_info: Dict[str, Dict] = {}  # Latest info per vessel
lock = Lock()  # Thread-safe access to shared data

# Kafka config
KAFKA_BOOTSTRAP = "localhost:19092"
KAFKA_TOPIC = "vessel_tracks"
KAFKA_GROUP_ID = "dash_vessel_viewer"

class VesselTracker:
    """Manages vessel position data and Kafka consumption."""
    
    def __init__(self):
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.running = False
        self.consumer_task: Optional[asyncio.Task] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        
    async def start_kafka_consumer(self) -> None:
        """Start consuming vessel data from Kafka asynchronously."""
        try:
            self.consumer = AIOKafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest'  # Start from latest messages
            )
            
            await self.consumer.start()
            logger.info(f"Connected to Kafka topic '{KAFKA_TOPIC}' on {KAFKA_BOOTSTRAP}")
            
            self.running = True
            async for message in self.consumer:
                if not self.running:
                    break
                    
                vessel_data = message.value
                self.add_vessel_position(vessel_data)
                
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}")
        finally:
            if self.consumer:
                await self.consumer.stop()
                logger.info("Kafka consumer stopped")
    
    def add_vessel_position(self, vessel_data: Dict) -> None:
        """Add a new position for a vessel."""
        if not vessel_data or 'mmsi' not in vessel_data:
            return
            
        mmsi = str(vessel_data['mmsi'])
        
        with lock:
            # Update vessel info
            vessel_info[mmsi] = vessel_data
            
            # Add position to history
            vessel_positions[mmsi].append(vessel_data)
            
            # Prune old vessels if we have too many
            if len(vessel_info) > MAX_VESSELS:
                # Remove oldest vessel (based on last position time)
                oldest_mmsi = min(
                    vessel_info.keys(),
                    key=lambda x: vessel_info[x].get('received_at', '')
                )
                del vessel_info[oldest_mmsi]
                del vessel_positions[oldest_mmsi]
    
    def get_vessels_df(self, 
                      min_speed: float = 0.0,
                      vessel_types: Optional[Set[str]] = None,
                      max_age_minutes: int = 30) -> pd.DataFrame:
        """Get vessel positions as a pandas DataFrame for plotting."""
        with lock:
            cutoff_time = datetime.now() - timedelta(minutes=max_age_minutes)
            
            # Collect all valid positions
            positions = []
            for mmsi, info in vessel_info.items():
                # Skip if vessel type filter is active and doesn't match
                if vessel_types and info.get('vessel_type') not in vessel_types:
                    continue
                    
                # Skip if speed filter is active and doesn't match
                if min_speed > 0 and float(info.get('sog', 0)) < min_speed:
                    continue
                    
                # Skip if position is too old
                try:
                    pos_time = datetime.fromisoformat(info.get('received_at', '').replace('Z', ''))
                    if pos_time < cutoff_time:
                        continue
                except (ValueError, TypeError):
                    continue
                
                positions.append(info)
            
            if not positions:
                return pd.DataFrame()
            
            df = pd.DataFrame(positions)
            
            # Ensure required columns exist
            required_cols = ['lat', 'lon', 'mmsi', 'vessel_name', 'sog', 'cog']
            for col in required_cols:
                if col not in df.columns:
                    df[col] = None
            
            return df
    
    def get_vessel_types(self) -> List[str]:
        """Get list of unique vessel types."""
        with lock:
            types = {info.get('vessel_type', 'unknown') for info in vessel_info.values()}
            return sorted(list(types))
    
    def stop(self) -> None:
        """Stop the Kafka consumer synchronously."""
        self.running = False
        if self.consumer_task and self.loop:
            self.loop.call_soon_threadsafe(self.consumer_task.cancel)
            # Wait for the task to be cancelled
            while not self.consumer_task.done():
                time.sleep(0.1)

# Initialize the tracker
tracker = VesselTracker()

# ---------------------------------------------------------
# ASYNC CONSUMER LOOP
# ---------------------------------------------------------
async def consume_kafka():
    """Main async Kafka consumer loop."""
    await tracker.start_kafka_consumer()

def start_consumer_loop():
    """Start the async consumer loop in a separate thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    tracker.loop = loop
    tracker.consumer_task = loop.create_task(consume_kafka())
    try:
        loop.run_until_complete(tracker.consumer_task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()

# ---------------------------------------------------------
# DASH APP SETUP & LAYOUT
# ---------------------------------------------------------
app = dash.Dash(__name__)
app.title = "Global Vessel Tracker"

# Define the layout
app.layout = html.Div([
    html.Div([
        html.H1("🌊 Global Vessel Real-Time Tracker", 
                style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '20px'}),
        
        # Filters
        html.Div([
            html.Div([
                html.H3("Filters", style={'color': '#34495e'}),
                html.Div([
                    html.Label("Minimum Speed (knots):"),
                    dcc.Slider(
                        id='speed-filter',
                        min=0,
                        max=30,
                        step=1,
                        value=0,
                        marks={i: str(i) for i in range(0, 31, 5)},
                    ),
                ], style={'marginBottom': '20px'}),
                
                html.Div([
                    html.Label("Vessel Types:"),
                    dcc.Dropdown(
                        id='vessel-type-filter',
                        multi=True,
                        style={'width': '100%'}
                    ),
                ], style={'marginBottom': '20px'}),
                
                html.Div([
                    html.Label("Max Age (minutes):"),
                    dcc.Slider(
                        id='age-filter',
                        min=1,
                        max=60,
                        step=1,
                        value=30,
                        marks={i: str(i) for i in range(0, 61, 10)},
                    ),
                ]),
            ], className='four columns', style={'padding': '20px'}),
            
            # Stats
            html.Div([
                html.H3("Statistics", style={'color': '#34495e'}),
                html.Div(id='vessel-stats', style={'fontSize': '14px'})
            ], className='four columns', style={'padding': '20px'}),
            
            # Connection status
            html.Div([
                html.H3("Connection Status", style={'color': '#34495e'}),
                html.Div(id='connection-status', style={'fontSize': '14px'})
            ], className='four columns', style={'padding': '20px'}),
        ], className='row', style={'marginBottom': '20px'}),
        
    ], style={'padding': '20px'}),
    
    # Map container
    html.Div([
        dcc.Graph(id='vessel-map', style={'height': '70vh'})
    ], style={'padding': '0 20px'}),
    
    # Auto-refresh interval
    dcc.Interval(
        id='interval-component',
        interval=2000,  # Update every 2 seconds
        n_intervals=0
    )
], style={'fontFamily': 'Arial, sans-serif'})

@app.callback(
    [Output('vessel-map', 'figure'),
     Output('vessel-stats', 'children'),
     Output('connection-status', 'children'),
     Output('vessel-type-filter', 'options')],
    [Input('interval-component', 'n_intervals'),
     Input('speed-filter', 'value'),
     Input('vessel-type-filter', 'value'),
     Input('age-filter', 'value')]
)
def update_dashboard(n: int, min_speed: float, vessel_types: List[str], max_age: int) -> tuple:
    """Update the map and vessel information."""
    
    # Get current data with filters
    df = tracker.get_vessels_df(
        min_speed=min_speed,
        vessel_types=set(vessel_types) if vessel_types else None,
        max_age_minutes=max_age
    )
    
    # Create the map figure
    fig = go.Figure()
    
    if not df.empty and 'lat' in df.columns and 'lon' in df.columns:
        # Remove any rows with missing lat/lon
        df_clean = df.dropna(subset=['lat', 'lon'])
        
        if not df_clean.empty:
            # Add vessel markers with hover info
            fig.add_trace(go.Scattermapbox(
                lat=df_clean['lat'],
                lon=df_clean['lon'],
                mode='markers',
                marker=dict(
                    size=10,
                    color=df_clean['sog'].fillna(0),  # Color by speed
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title='Speed (knots)')
                ),
                text=df_clean.apply(
                    lambda x: f"Vessel: {x.get('vessel_name', 'Unknown')}<br>" +
                             f"MMSI: {x.get('mmsi', 'N/A')}<br>" +
                             f"Speed: {x.get('sog', 'N/A')} knots<br>" +
                             f"Course: {x.get('cog', 'N/A')}°<br>" +
                             f"Type: {x.get('vessel_type', 'unknown')}",
                    axis=1
                ),
                hoverinfo='text',
                name='Vessels'
            ))
            
            # Center map on mean position
            center_lat = df_clean['lat'].mean()
            center_lon = df_clean['lon'].mean()
        else:
            # Default center (San Francisco Bay area)
            center_lat, center_lon = 37.7749, -122.4194
    else:
        # Default center if no data
        center_lat, center_lon = 37.7749, -122.4194
    
    # Configure map layout
    fig.update_layout(
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=center_lat, lon=center_lon),
            zoom=5  # Start zoomed out for global view
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        showlegend=True,
        legend=dict(x=0.01, y=0.99)
    )
    
    # Vessel statistics
    stats = html.Div([
        html.P(f"Total Vessels: {len(df_clean) if not df_clean.empty else 0}"),
        html.P(f"Average Speed: {df_clean['sog'].mean():.1f} knots" if not df_clean.empty else "N/A"),
        html.P(f"Vessel Types: {', '.join(df_clean['vessel_type'].unique())}" if not df_clean.empty else "N/A"),
        html.P(f"Last Update: {datetime.now().strftime('%H:%M:%S')}"),
    ])
    
    # Connection status
    status_color = 'green' if tracker.running else 'orange'
    connection_status = html.Div([
        html.P(f"Status: {'Connected' if tracker.running else 'Disconnected'}", 
               style={'color': status_color, 'fontWeight': 'bold'}),
        html.P(f"Messages Received: {sum(len(positions) for positions in vessel_positions.values())}"),
        html.P(f"Active Vessels: {len(vessel_info)}"),
    ])
    
    # Vessel type filter options
    vessel_type_options = [
        {'label': vtype, 'value': vtype}
        for vtype in tracker.get_vessel_types()
    ]
    
    return fig, stats, connection_status, vessel_type_options

def main():
    """Start the Dash application."""
    # Start Kafka consumer in a separate thread
    consumer_thread = threading.Thread(target=start_consumer_loop, daemon=True)
    consumer_thread.start()
    
    try:
        # Run the Dash app
        print("[*] Starting Dash app on http://127.0.0.1:8050")
        app.run(debug=True, host='0.0.0.0', port=8050)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # Clean up synchronously
        tracker.stop()

if __name__ == '__main__':
    main() 