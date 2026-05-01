#!/usr/bin/env python3
"""
Real-time PHONIX vessel tracking dashboard using Dash and Plotly.

This application:
1. Consumes filtered PHONIX vessel data from Kafka topic 'phonix_track'
2. Displays vessel positions on an interactive map
3. Shows vessel trajectory over time
4. Updates in real-time as new position data arrives

Requirements:
- dash
- plotly
- aiokafka
- pandas

Usage:
    python phonix_map.py
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections import deque
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import dash
from dash import dcc, html, Input, Output, callback
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
# Store PHONIX vessel positions
positions = deque(maxlen=1000)  # Store up to 1000 positions
latest_position: Optional[Dict] = None
lock = Lock()  # Thread-safe access to shared data

# Kafka config
KAFKA_BOOTSTRAP = "localhost:19092"
KAFKA_TOPIC = "phonix_track"
KAFKA_GROUP_ID = "dash_phonix_viewer"

class PhonixTracker:
    """Manages PHONIX vessel position data and Kafka consumption."""
    
    def __init__(self):
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.running = False
        self.consumer_task: Optional[asyncio.Task] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        
    async def start_kafka_consumer(self) -> None:
        """Start consuming PHONIX data from Kafka asynchronously."""
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
                    
                position_data = message.value
                self.add_position(position_data)
                logger.info(f"Received position: {position_data}")
                
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}")
        finally:
            if self.consumer:
                await self.consumer.stop()
                logger.info("Kafka consumer stopped")
    
    def add_position(self, position_data: Dict) -> None:
        """Add a new position to the tracking data."""
        # Add timestamp if not present
        if 'received_at' not in position_data:
            position_data['received_at'] = datetime.now().isoformat()
            
        with lock:
            positions.append(position_data)
            global latest_position
            latest_position = position_data
    
    def get_positions_df(self) -> pd.DataFrame:
        """Get positions as a pandas DataFrame for plotting."""
        with lock:
            if not positions:
                return pd.DataFrame()
                
            df = pd.DataFrame(list(positions))
            
            # Ensure required columns exist
            required_cols = ['lat', 'lon', 'mmsi', 'vessel_name']
            for col in required_cols:
                if col not in df.columns:
                    df[col] = None
                    
            return df
    
    def get_latest_info(self) -> Dict:
        """Get latest vessel information for display."""
        with lock:
            if not latest_position:
                return {}
                
            return {
                'mmsi': latest_position.get('mmsi', 'N/A'),
                'vessel_name': latest_position.get('vessel_name', 'N/A'),
                'lat': latest_position.get('lat', 'N/A'),
                'lon': latest_position.get('lon', 'N/A'),
                'sog': latest_position.get('sog', 'N/A'),
                'cog': latest_position.get('cog', 'N/A'),
                'timestamp': latest_position.get('timestamp', 'N/A'),
                'total_positions': len(positions)
            }
    
    def stop(self) -> None:
        """Stop the Kafka consumer synchronously."""
        self.running = False
        if self.consumer_task and self.loop:
            self.loop.call_soon_threadsafe(self.consumer_task.cancel)
            # Wait for the task to be cancelled
            while not self.consumer_task.done():
                time.sleep(0.1)

# Initialize the tracker
tracker = PhonixTracker()

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
    tracker.loop = loop  # Store the loop reference
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
app.title = "PHONIX Vessel Tracker"

# Define the layout
app.layout = html.Div([
    html.Div([
        html.H1("🚢 PHONIX Vessel Real-Time Tracker", 
                style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '20px'}),
        
        html.Div([
            html.Div([
                html.H3("Vessel Information", style={'color': '#34495e'}),
                html.Div(id='vessel-info', style={'fontSize': '14px'})
            ], className='six columns'),
            
            html.Div([
                html.H3("Connection Status", style={'color': '#34495e'}),
                html.Div(id='connection-status', style={'fontSize': '14px'})
            ], className='six columns'),
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
     Output('vessel-info', 'children'),
     Output('connection-status', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_dashboard(n: int) -> tuple:
    """Update the map and vessel information."""
    
    # Get current data
    df = tracker.get_positions_df()
    latest_info = tracker.get_latest_info()
    
    # Create the map figure
    fig = go.Figure()
    
    if not df.empty and 'lat' in df.columns and 'lon' in df.columns:
        # Remove any rows with missing lat/lon
        df_clean = df.dropna(subset=['lat', 'lon'])
        
        if not df_clean.empty:
            # Add trajectory line
            fig.add_trace(go.Scattermapbox(
                lat=df_clean['lat'],
                lon=df_clean['lon'],
                mode='lines+markers',
                marker=dict(size=8, color='blue'),
                line=dict(width=2, color='lightblue'),
                name='Trajectory',
                hovertemplate='<b>Position %{pointNumber}</b><br>' +
                            'Lat: %{lat}<br>' +
                            'Lon: %{lon}<br>' +
                            '<extra></extra>'
            ))
            
            # Highlight latest position
            if len(df_clean) > 0:
                latest_lat = df_clean.iloc[-1]['lat']
                latest_lon = df_clean.iloc[-1]['lon']
                
                fig.add_trace(go.Scattermapbox(
                    lat=[latest_lat],
                    lon=[latest_lon],
                    mode='markers',
                    marker=dict(size=15, color='red', symbol='circle'),
                    name='Current Position',
                    hovertemplate='<b>Current Position</b><br>' +
                                'Lat: %{lat}<br>' +
                                'Lon: %{lon}<br>' +
                                '<extra></extra>'
                ))
                
                # Center map on latest position
                center_lat, center_lon = latest_lat, latest_lon
            else:
                # Default center (San Francisco Bay area)
                center_lat, center_lon = 37.7749, -122.4194
        else:
            # Default center if no valid data
            center_lat, center_lon = 37.7749, -122.4194
    else:
        # Default center if no data
        center_lat, center_lon = 37.7749, -122.4194
    
    # Configure map layout
    fig.update_layout(
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=center_lat, lon=center_lon),
            zoom=10
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        showlegend=True,
        legend=dict(x=0.01, y=0.99)
    )
    
    # Vessel information display
    vessel_info = html.Div([
        html.P(f"MMSI: {latest_info.get('mmsi', 'N/A')}"),
        html.P(f"Vessel Name: {latest_info.get('vessel_name', 'N/A')}"),
        html.P(f"Latitude: {latest_info.get('lat', 'N/A')}"),
        html.P(f"Longitude: {latest_info.get('lon', 'N/A')}"),
        html.P(f"Speed (SOG): {latest_info.get('sog', 'N/A')} knots"),
        html.P(f"Course (COG): {latest_info.get('cog', 'N/A')}°"),
        html.P(f"Last Update: {latest_info.get('timestamp', 'N/A')}"),
    ])
    
    # Connection status
    status_color = 'green' if latest_info.get('total_positions', 0) > 0 else 'orange'
    connection_status = html.Div([
        html.P(f"Total Positions: {latest_info.get('total_positions', 0)}"),
        html.P(f"Status: {'Connected' if tracker.running else 'Disconnected'}", 
               style={'color': status_color, 'fontWeight': 'bold'}),
        html.P(f"Last Update: {datetime.now().strftime('%H:%M:%S')}"),
    ])
    
    return fig, vessel_info, connection_status

def main():
    """Start the Dash application."""
    # Start Kafka consumer in a separate thread
    consumer_thread = threading.Thread(target=start_consumer_loop, daemon=True)
    consumer_thread.start()
    
    try:
        # Run the Dash app using the new run() method
        print("[*] Starting Dash app on http://127.0.0.1:8050")
        app.run(debug=True, host='0.0.0.0', port=8050)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # Clean up synchronously
        tracker.stop()

if __name__ == '__main__':
    main() 