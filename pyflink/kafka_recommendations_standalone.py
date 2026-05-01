#!/usr/bin/env python3
"""
Standalone Kafka Recommendations Service

A simple Kafka consumer/producer that processes basket events and generates recommendations
without requiring a full Flink cluster. This is easier to test and debug.

Usage:
    python kafka_recommendations_standalone.py [--bootstrap-servers localhost:19092]
"""

import json
import logging
import argparse
import asyncio
from typing import Dict, List, Any
from datetime import datetime

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StandaloneRecommendationEngine:
    """
    Standalone recommendation engine using Kafka directly
    """
    
    def __init__(self, bootstrap_servers: str, input_topic: str, output_topic: str, consumer_group: str):
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.consumer_group = consumer_group
        
        self.consumer = None
        self.producer = None
        
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
    
    async def start(self):
        """Start the Kafka consumer and producer"""
        # Initialize consumer
        self.consumer = AIOKafkaConsumer(
            self.input_topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.consumer_group,
            value_deserializer=lambda m: m.decode("utf-8"),
            auto_offset_reset="latest"
        )
        
        # Initialize producer
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Start both
        await self.consumer.start()
        await self.producer.start()
        
        logger.info(f"Started Kafka consumer for topic: {self.input_topic}")
        logger.info(f"Started Kafka producer for topic: {self.output_topic}")
        logger.info(f"Bootstrap servers: {self.bootstrap_servers}")
        logger.info(f"Consumer group: {self.consumer_group}")
    
    async def stop(self):
        """Stop the Kafka consumer and producer"""
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        logger.info("Stopped Kafka consumer and producer")
    
    async def process_events(self):
        """Process incoming basket events and generate recommendations"""
        logger.info("Starting to process basket events...")
        
        try:
            async for msg in self.consumer:
                try:
                    # Parse the basket event
                    event_data = msg.value
                    logger.info(f"Received message: {event_data[:100]}...")
                    
                    event = json.loads(event_data)
                    
                    # Generate recommendations
                    recommendations = await self._generate_recommendations(event)
                    
                    # Send recommendations back
                    await self.producer.send_and_wait(self.output_topic, recommendations)
                    
                    logger.info(f"Sent {len(recommendations.get('recommendations', []))} recommendations for session {recommendations.get('session_id', 'unknown')}")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except Exception as e:
            logger.error(f"Error in event processing loop: {e}")
    
    async def _generate_recommendations(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Generate recommendations based on basket event"""
        event_type = event.get("event_type", "")
        product_id = event.get("product_id", "")
        product_name = event.get("product_name", "Unknown Product")
        session_id = event.get("session_id", "")
        
        logger.info(f"Processing {event_type} event for {product_name} (ID: {product_id}) session: {session_id[:8]}...")
        
        # Get recommendations for this product
        recommendations = await self._get_recommendations(product_id, event_type)
        
        # Create response
        response = {
            "session_id": session_id,  # Use the session ID from the basket event
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "recommendations": recommendations,
            "reasoning": f"AI recommendations for {product_name}"
        }
        
        logger.info(f"Generated {len(recommendations)} recommendations for session {session_id[:8]}...")
        
        return response
    
    async def _get_recommendations(self, product_id: str, event_type: str) -> List[Dict[str, Any]]:
        """Get recommendations for a product"""
        if product_id in self.recommendations_map:
            # Get specific recommendations for this product - create fresh copies
            recommendations = []
            for rec in self.recommendations_map[product_id][:3]:
                recommendations.append({
                    "name": rec["name"],
                    "reason": rec["reason"],
                    "price": rec["price"]
                })
        else:
            # Use general recommendations - create fresh copies
            recommendations = []
            for rec in self.general_recommendations[:3]:
                recommendations.append({
                    "name": rec["name"],
                    "reason": rec["reason"],
                    "price": rec["price"]
                })
        
        # Add event-specific reasoning
        if event_type == "add":
            for rec in recommendations:
                rec["reason"] = f"Since you added an item: {rec['reason']}"
        elif event_type == "checkout":
            for rec in recommendations:
                rec["reason"] = f"For your next order: {rec['reason']}"
        elif event_type == "update":
            for rec in recommendations:
                rec["reason"] = f"You might also like: {rec['reason']}"
        
        return recommendations

async def main():
    parser = argparse.ArgumentParser(description="Standalone Kafka Recommendations Service")
    parser.add_argument('--bootstrap-servers', default='localhost:19092',
                       help='Kafka bootstrap servers')
    parser.add_argument('--input-topic', default='basket-events-v2',
                       help='Input Kafka topic for basket events')
    parser.add_argument('--output-topic', default='recommendations-v2',
                       help='Output Kafka topic for recommendations')
    parser.add_argument('--consumer-group', default='standalone-recommendations',
                       help='Kafka consumer group ID')
    
    args = parser.parse_args()
    
    # Create recommendation engine
    engine = StandaloneRecommendationEngine(
        bootstrap_servers=args.bootstrap_servers,
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        consumer_group=args.consumer_group
    )
    
    logger.info("Starting Standalone Kafka Recommendations Service")
    
    try:
        # Start the service
        await engine.start()
        
        # Process events indefinitely
        await engine.process_events()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Service error: {e}")
    finally:
        # Clean shutdown
        await engine.stop()
        logger.info("Service stopped")

if __name__ == "__main__":
    asyncio.run(main()) 