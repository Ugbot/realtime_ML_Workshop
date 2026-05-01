#!/usr/bin/env python3
"""
Test data generator for PHONIX vessel tracking.

Generates realistic AIS messages for the PHONIX vessel and sends them to Kafka
for testing the dashboard visualization.

Usage:
    python test_data_generator.py
"""

from __future__ import annotations

import json
import time
import random
from datetime import datetime, timezone
from typing import Dict

from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PhonixDataGenerator:
    """Generates realistic PHONIX vessel movement data."""
    
    def __init__(self, bootstrap_servers: str = 'localhost:19092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Starting position (San Francisco Bay area)
        self.current_lat = 37.7749
        self.current_lon = -122.4194
        self.current_cog = 90.0  # Course over ground (degrees)
        self.current_sog = 8.5   # Speed over ground (knots)
        
    def generate_position(self) -> Dict:
        """Generate a realistic next position for PHONIX."""
        
        # Simulate realistic vessel movement
        # Small random changes in course and speed
        course_change = random.uniform(-5, 5)  # degrees
        speed_change = random.uniform(-0.5, 0.5)  # knots
        
        self.current_cog = (self.current_cog + course_change) % 360
        self.current_sog = max(0, min(15, self.current_sog + speed_change))
        
        # Calculate new position based on course and speed
        # Approximate movement (simplified calculation)
        speed_ms = self.current_sog * 0.514444  # knots to m/s
        time_step = 10  # seconds between updates
        distance_m = speed_ms * time_step
        
        # Convert to lat/lon changes (very approximate)
        lat_change = (distance_m * 0.000009) * random.uniform(0.8, 1.2)
        lon_change = (distance_m * 0.000009) * random.uniform(0.8, 1.2)
        
        # Apply course direction
        import math
        course_rad = math.radians(self.current_cog)
        self.current_lat += lat_change * math.cos(course_rad)
        self.current_lon += lon_change * math.sin(course_rad)
        
        # Keep within reasonable bounds (San Francisco Bay area)
        self.current_lat = max(37.5, min(38.0, self.current_lat))
        self.current_lon = max(-122.8, min(-122.0, self.current_lon))
        
        return {
            "mmsi": 368381530,
            "vessel_name": "PHONIX",
            "lat": round(self.current_lat, 6),
            "lon": round(self.current_lon, 6),
            "sog": round(self.current_sog, 1),
            "cog": round(self.current_cog, 1),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    def send_to_kafka(self, topic: str, position_data: Dict) -> None:
        """Send position data to Kafka topic."""
        try:
            future = self.producer.send(topic, position_data)
            future.get(timeout=10)  # Wait for send to complete
            logger.info(f"Sent to {topic}: {position_data}")
        except Exception as e:
            logger.error(f"Failed to send to Kafka: {e}")
    
    def run_simulation(self, 
                      ais_topic: str = 'ais',
                      interval_seconds: int = 10,
                      duration_minutes: int = 60) -> None:
        """Run the simulation for specified duration."""
        
        logger.info(f"Starting PHONIX simulation for {duration_minutes} minutes")
        logger.info(f"Sending to topic '{ais_topic}' every {interval_seconds} seconds")
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        try:
            while time.time() < end_time:
                position = self.generate_position()
                self.send_to_kafka(ais_topic, position)
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("Simulation stopped by user")
        finally:
            self.producer.close()
            logger.info("Simulation ended")

def main():
    """Run the test data generator."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate test PHONIX vessel data")
    parser.add_argument('--bootstrap-servers', default='localhost:19092',
                       help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='ais',
                       help='Kafka topic to send AIS data to')
    parser.add_argument('--interval', type=int, default=10,
                       help='Interval between messages (seconds)')
    parser.add_argument('--duration', type=int, default=60,
                       help='Duration to run simulation (minutes)')
    
    args = parser.parse_args()
    
    generator = PhonixDataGenerator(args.bootstrap_servers)
    generator.run_simulation(
        ais_topic=args.topic,
        interval_seconds=args.interval,
        duration_minutes=args.duration
    )

if __name__ == '__main__':
    main() 