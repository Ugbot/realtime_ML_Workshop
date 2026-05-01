# PHONIX Vessel Real-Time Tracking Dashboard

A real-time vessel tracking system that uses PyFlink to process AIS data and Dash to visualize vessel movements on an interactive map.

## System Overview

```
AIS Data → Kafka → PyFlink Filter → Kafka → Dash Dashboard
```

1. **AIS Data Source**: Raw AIS messages containing vessel positions
2. **PyFlink Job**: Filters messages for the PHONIX vessel (MMSI: 368381530)
3. **Kafka Topics**: 
   - `ais`: Raw AIS data
   - `phonix_track`: Filtered PHONIX positions
4. **Dash Dashboard**: Real-time map visualization

## Prerequisites

- Python 3.8+
- Apache Kafka running on `localhost:19092`
- PyFlink with Kafka connector JARs

## Installation

1. **Install Python dependencies:**
   ```bash
   cd dash_app
   pip install -r requirements.txt
   ```

2. **Install PyFlink dependencies:**
   ```bash
   pip install apache-flink
   ```

3. **Ensure Kafka connector JARs are available:**
   - `flink-connector-kafka-3.3.0-1.20.jar`
   - `kafka-clients-3.6.1.jar`

## Usage

### Step 1: Start the PyFlink Processing Job

```bash
cd ../pyflink
python pheonix.py --source-topic ais --sink-topic phonix_track
```

This will:
- Connect to Kafka on `localhost:19092`
- Listen for AIS messages on the `ais` topic
- Filter for PHONIX vessel (MMSI: 368381530)
- Output filtered data to `phonix_track` topic

### Step 2: Start the Dash Dashboard

```bash
cd dash_app
python phonix_map.py
```

The dashboard will be available at: `http://localhost:8050`

### Step 3: Generate Test Data (Optional)

To test the system with simulated PHONIX movements:

```bash
cd dash_app
python test_data_generator.py --topic ais --interval 5 --duration 30
```

This generates realistic vessel movement data every 5 seconds for 30 minutes.

## Dashboard Features

### 🗺️ Interactive Map
- **Real-time vessel tracking** with position updates every 2 seconds
- **Trajectory visualization** showing the vessel's path over time
- **Current position highlighting** with a red marker
- **OpenStreetMap** base layer for detailed geographic context

### 📊 Vessel Information Panel
- MMSI and vessel name
- Current latitude/longitude coordinates
- Speed over ground (SOG) in knots
- Course over ground (COG) in degrees
- Last position timestamp

### 🔌 Connection Status
- Real-time connection status to Kafka
- Total number of positions received
- Last update timestamp

## Configuration

### Kafka Settings
- **Bootstrap Servers**: `localhost:19092`
- **Source Topic**: `ais` (raw AIS data)
- **Sink Topic**: `phonix_track` (filtered PHONIX data)
- **Consumer Group**: `dash_phonix_viewer`

### Dashboard Settings
- **Update Interval**: 2 seconds
- **Max Positions**: 1000 (configurable in `PhonixTracker`)
- **Default Map Center**: San Francisco Bay (37.7749, -122.4194)

## Data Format

### Input AIS Message Format
```json
{
  "mmsi": 368381530,
  "vessel_name": "PHONIX",
  "lat": 37.7749,
  "lon": -122.4194,
  "sog": 11.2,
  "cog": 270.3,
  "timestamp": "2025-05-27T13:45:05Z"
}
```

### Output Format
Same as input, with additional `received_at` timestamp added by the dashboard.

## Troubleshooting

### Common Issues

1. **Kafka Connection Failed**
   - Ensure Kafka is running on `localhost:19092`
   - Check if topics exist: `kafka-topics --list --bootstrap-server localhost:19092`

2. **PyFlink Job Fails**
   - Verify JAR files are in the correct location
   - Check Kafka connector compatibility with your Flink version

3. **Dashboard Shows No Data**
   - Verify PyFlink job is running and processing messages
   - Check Kafka topic has messages: `kafka-console-consumer --topic phonix_track --bootstrap-server localhost:19092`

4. **Map Not Loading**
   - Check internet connection (required for OpenStreetMap tiles)
   - Verify browser console for JavaScript errors

### Logs

- **PyFlink**: Check console output for processing logs
- **Dashboard**: Check console output for Kafka consumer logs
- **Test Generator**: Monitor message sending status

## Customization

### Changing the Tracked Vessel
Edit `pyflink/pheonix.py`:
```python
PHONIX_MMSI = 123456789  # Your vessel's MMSI
PHONIX_NAME = "YOUR_VESSEL"  # Your vessel's name
```

### Adjusting Map Settings
Edit `dash_app/phonix_map.py`:
```python
# Update interval (milliseconds)
interval=5000,  # 5 seconds

# Map zoom level
zoom=12  # Closer zoom

# Map style
style="satellite-streets"  # Different map style
```

### Adding More Vessel Information
Extend the vessel info panel in the `update_dashboard` callback to display additional AIS fields like heading, navigation status, etc.

## Performance Notes

- The dashboard stores up to 1000 position points in memory
- Map updates every 2 seconds to balance responsiveness and performance
- Kafka consumer uses latest offset to avoid processing historical data on startup

## License

This project is for educational and demonstration purposes. 