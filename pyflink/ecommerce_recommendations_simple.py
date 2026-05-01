#!/usr/bin/env python3
"""
Simple E-commerce Recommendations Flink Job

A basic version that consumes basket events and produces simple recommendations
using the modern KafkaSource/KafkaSink approach from topic_splitter.py.

Usage:
    python ecommerce_recommendations_simple.py [--bootstrap-servers localhost:19092]
"""

import json
import logging
import argparse
from typing import Dict, List, Any
from datetime import datetime
from pathlib import Path

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.datastream.functions import MapFunction
from pyflink.common.serialization import SimpleStringSchema

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Hardcoded defaults from hello_kafka.py
DEFAULT_BOOTSTRAP_SERVERS = "localhost:19092"
DEFAULT_INPUT_TOPIC = "basket-events-v2"
DEFAULT_OUTPUT_TOPIC = "recommendations-v2"
DEFAULT_CONSUMER_GROUP = "flink-recommendations-simple-v2"

# JAR file paths (same as hello_kafka.py)
KAFKA_CONNECTOR_JAR = "file:///Users/bengamble/realtime_ML_Workshop/pyflink/flink-connector-kafka-3.3.0-1.20.jar"
KAFKA_CLIENTS_JAR = "file:///Users/bengamble/realtime_ML_Workshop/pyflink/kafka-clients-3.6.1.jar"

class SimpleRecommendationEngine(MapFunction):
    """
    Simple recommendation engine that processes basket events
    and generates basic product recommendations
    """
    
    def __init__(self):
        # Simple product recommendations mapping
        self.recommendations_map = {
            "laptop-001": [
                {"name": "Wireless Mouse", "reason": "Perfect for your laptop", "price": 25.99},
                {"name": "Laptop Bag", "reason": "Protect your investment", "price": 49.99},
                {"name": "External Monitor", "reason": "Expand your workspace", "price": 199.99}
            ],
            "phone-001": [
                {"name": "Phone Case", "reason": "Essential protection", "price": 19.99},
                {"name": "Wireless Charger", "reason": "Convenient charging", "price": 29.99},
                {"name": "Bluetooth Earbuds", "reason": "Great for calls and music", "price": 79.99}
            ],
            "headphones-001": [
                {"name": "Headphone Stand", "reason": "Keep them organized", "price": 24.99},
                {"name": "Audio Cable", "reason": "Backup connection", "price": 15.99},
                {"name": "Carrying Case", "reason": "Travel protection", "price": 34.99}
            ],
            "book-001": [
                {"name": "Notebook", "reason": "Take notes while reading", "price": 8.99},
                {"name": "Reading Light", "reason": "Read anywhere", "price": 29.99},
                {"name": "Bookmark Set", "reason": "Mark your progress", "price": 12.99}
            ],
            "coffee-001": [
                {"name": "Coffee Grinder", "reason": "Fresh ground coffee", "price": 89.99},
                {"name": "French Press", "reason": "Perfect brewing method", "price": 29.99},
                {"name": "Coffee Mug", "reason": "Enjoy your coffee", "price": 14.99}
            ],
            "watch-001": [
                {"name": "Watch Band", "reason": "Change your style", "price": 39.99},
                {"name": "Screen Protector", "reason": "Protect the display", "price": 9.99},
                {"name": "Charging Dock", "reason": "Convenient charging", "price": 49.99}
            ]
        }
        
        # General recommendations for unknown products or empty carts
        self.general_recommendations = [
            {"name": "Wireless Earbuds", "reason": "Most popular this week", "price": 79.99},
            {"name": "Phone Charger", "reason": "Always useful to have", "price": 24.99},
            {"name": "Blue Light Glasses", "reason": "Great for computer users", "price": 34.99}
        ]
    
    def map(self, value: str) -> str:
        """Process basket event string and generate recommendations"""
        try:
            logger.info(f"Processing message: {value[:100]}...")
            
            # Parse the basket event
            event = json.loads(value)
            
            event_type = event.get("event_type", "")
            product_id = event.get("product_id", "")
            product_name = event.get("product_name", "Unknown Product")
            session_id = event.get("session_id", "")
            
            logger.info(f"Event: {event_type} for {product_name} (ID: {product_id})")
            
            # Generate recommendations based on the product
            recommendations = self._get_recommendations(product_id, event_type)
            
            # Create response message
            response = {
                "session_id": session_id,
                "timestamp": datetime.now().isoformat(),
                "event_type": event_type,
                "recommendations": recommendations,
                "reasoning": f"Simple recommendations for {product_name}"
            }
            
            result = json.dumps(response)
            logger.info(f"Generated {len(recommendations)} recommendations")
            
            return result
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
            return self._create_error_response("Invalid JSON")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return self._create_error_response(str(e))
    
    def _get_recommendations(self, product_id: str, event_type: str) -> List[Dict[str, Any]]:
        """Get recommendations for a product"""
        if product_id in self.recommendations_map:
            # Get specific recommendations for this product
            recommendations = self.recommendations_map[product_id][:3]  # Limit to 3
        else:
            # Use general recommendations
            recommendations = self.general_recommendations[:3]
        
        # Add event-specific reasoning
        if event_type == "add":
            for rec in recommendations:
                rec["reason"] = f"Since you added a product: {rec['reason']}"
        elif event_type == "checkout":
            for rec in recommendations:
                rec["reason"] = f"For your next order: {rec['reason']}"
        
        return recommendations
    
    def _create_error_response(self, error_msg: str) -> str:
        """Create error response"""
        return json.dumps({
            "session_id": "",
            "timestamp": datetime.now().isoformat(),
            "recommendations": [],
            "error": error_msg
        })

def setup_environment_with_jars(env: StreamExecutionEnvironment) -> None:
    """Setup Flink environment with required JAR files (same as hello_kafka.py)"""
    try:
        # Check if JAR files exist
        kafka_connector_path = Path(KAFKA_CONNECTOR_JAR.replace("file://", ""))
        kafka_clients_path = Path(KAFKA_CLIENTS_JAR.replace("file://", ""))
        
        if not kafka_connector_path.exists():
            logger.warning(f"Kafka connector JAR not found at {kafka_connector_path}")
            logger.warning("Flink job may fail without proper Kafka connector")
        
        if not kafka_clients_path.exists():
            logger.warning(f"Kafka clients JAR not found at {kafka_clients_path}")
            logger.warning("Flink job may fail without proper Kafka clients")
        
        # Add JARs to environment (same as hello_kafka.py and topic_splitter.py)
        env.add_jars(KAFKA_CONNECTOR_JAR, KAFKA_CLIENTS_JAR)
        logger.info(f"Added JAR files: {KAFKA_CONNECTOR_JAR}, {KAFKA_CLIENTS_JAR}")
        
    except Exception as e:
        logger.error(f"Error setting up JAR files: {e}")
        logger.warning("Continuing without JAR setup - job may fail")

def main():
    parser = argparse.ArgumentParser(description="Simple E-commerce Recommendations Flink Job")
    parser.add_argument('--bootstrap-servers', default=DEFAULT_BOOTSTRAP_SERVERS,
                       help=f'Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP_SERVERS})')
    parser.add_argument('--input-topic', default=DEFAULT_INPUT_TOPIC,
                       help=f'Input Kafka topic for basket events (default: {DEFAULT_INPUT_TOPIC})')
    parser.add_argument('--output-topic', default=DEFAULT_OUTPUT_TOPIC,
                       help=f'Output Kafka topic for recommendations (default: {DEFAULT_OUTPUT_TOPIC})')
    parser.add_argument('--consumer-group', default=DEFAULT_CONSUMER_GROUP,
                       help=f'Kafka consumer group ID (default: {DEFAULT_CONSUMER_GROUP})')
    
    args = parser.parse_args()
    
    logger.info(f"Starting Simple E-commerce Recommendations job")
    logger.info(f"Bootstrap servers: {args.bootstrap_servers}")
    logger.info(f"Input topic: {args.input_topic}")
    logger.info(f"Output topic: {args.output_topic}")
    logger.info(f"Consumer group: {args.consumer_group}")
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Setup JAR files (same as hello_kafka.py and topic_splitter.py)
    setup_environment_with_jars(env)
    
    # Create Kafka source (using working approach from topic_splitter.py)
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(args.bootstrap_servers)
        .set_topics(args.input_topic)
        .set_group_id(args.consumer_group)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())  # This works!
        .build()
    )
    
    # Create Kafka sink (using working approach from topic_splitter.py)
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(args.bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(args.output_topic)
            .set_value_serialization_schema(SimpleStringSchema())  # This works!
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    
    # Create data stream and process (using working approach)
    logger.info("Setting up data stream...")
    recommendations = (
        env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "basket_events_source")
        .map(SimpleRecommendationEngine())
    )
    
    # Send recommendations to output topic
    recommendations.sink_to(kafka_sink)
    
    logger.info("Starting job execution...")
    
    # Execute the job
    try:
        env.execute("Simple E-commerce Recommendations")
    except Exception as e:
        logger.error(f"Job execution failed: {e}")
        logger.info("Tip: Try using the standalone service instead:")
        logger.info("  python3 kafka_recommendations_standalone.py")
        raise

if __name__ == "__main__":
    main() 