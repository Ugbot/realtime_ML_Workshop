#!/usr/bin/env python3
"""
E-commerce Recommendations Flink Job

This job consumes basket events from Kafka and produces personalized 
recommendations using simulated LLM calls. In a real implementation,
this would call an actual LLM API like OpenAI, Claude, or a local model.

Usage:
    python ecommerce_recommendations.py [--bootstrap-servers localhost:19092]
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
DEFAULT_CONSUMER_GROUP = "flink-recommendations-v2"

# JAR file paths (same as hello_kafka.py)
KAFKA_CONNECTOR_JAR = "file:///Users/bengamble/realtime_ML_Workshop/pyflink/flink-connector-kafka-3.3.0-1.20.jar"
KAFKA_CLIENTS_JAR = "file:///Users/bengamble/realtime_ML_Workshop/pyflink/kafka-clients-3.6.1.jar"

class RecommendationEngine(MapFunction):
    """
    Simulated AI recommendation engine that processes basket events
    and generates product recommendations
    """
    
    def __init__(self):
        # Product knowledge base for recommendations
        self.product_knowledge = {
            "laptop-001": {
                "category": "Electronics",
                "complements": ["mouse", "laptop bag", "external monitor", "wireless keyboard"],
                "upgrades": ["gaming laptop", "workstation laptop"],
                "accessories": ["laptop stand", "cooling pad", "USB hub"]
            },
            "phone-001": {
                "category": "Electronics", 
                "complements": ["phone case", "screen protector", "wireless charger", "power bank"],
                "upgrades": ["phone with more storage", "latest model"],
                "accessories": ["bluetooth earbuds", "car mount", "selfie stick"]
            },
            "headphones-001": {
                "category": "Electronics",
                "complements": ["headphone stand", "audio cable", "foam ear tips"],
                "upgrades": ["premium headphones", "studio monitors"],
                "accessories": ["carrying case", "cleaning kit"]
            },
            "book-001": {
                "category": "Books",
                "complements": ["notebook", "highlighters", "bookmarks"],
                "upgrades": ["advanced programming books", "latest edition"],
                "accessories": ["reading light", "book stand"]
            },
            "coffee-001": {
                "category": "Food & Beverages",
                "complements": ["coffee grinder", "french press", "coffee filters"],
                "upgrades": ["premium single origin", "espresso blend"],
                "accessories": ["coffee mug", "thermal carafe", "coffee scale"]
            },
            "watch-001": {
                "category": "Electronics",
                "complements": ["watch band", "screen protector", "charging dock"],
                "upgrades": ["premium smartwatch", "fitness tracker"],
                "accessories": ["watch case", "cleaning cloth"]
            }
        }
        
        self.category_trends = {
            "Electronics": ["sustainability", "wireless", "AI-powered", "ergonomic"],
            "Books": ["latest editions", "companion guides", "practical exercises"],
            "Food & Beverages": ["organic", "fair trade", "artisanal", "health-focused"]
        }
    
    def map(self, value: str) -> str:
        """Process basket event string and generate recommendations (fixed JSON handling)"""
        try:
            # Parse the basket event (using string input like topic_splitter.py)
            event = json.loads(value)
            
            logger.info(f"Processing basket event: {event.get('event_type')} for {event.get('product_name')}")
            
            # Generate recommendations based on event type and products
            recommendations = self._generate_recommendations(event)
            
            # Create response message
            response = {
                "session_id": event.get("session_id", ""),
                "timestamp": datetime.now().isoformat(),
                "event_type": event.get("event_type"),
                "recommendations": recommendations,
                "reasoning": "AI-generated based on cart contents and purchase patterns"
            }
            
            logger.info(f"Generated {len(recommendations)} recommendations")
            return json.dumps(response)
            
        except Exception as e:
            logger.error(f"Error processing basket event: {e}")
            # Return empty recommendations on error
            return json.dumps({
                "session_id": "",
                "timestamp": datetime.now().isoformat(),
                "recommendations": [],
                "error": str(e)
            })
    
    def _generate_recommendations(self, event: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate product recommendations based on basket event"""
        recommendations = []
        
        # Get current cart items
        cart_items = event.get("cart_items", [])
        event_type = event.get("event_type", "")
        
        if not cart_items:
            # Return general popular recommendations if cart is empty
            return self._get_popular_recommendations()
        
        # Analyze cart contents
        categories_in_cart = set()
        products_in_cart = []
        
        for item in cart_items:
            product_id = item.get("product_id", "")
            products_in_cart.append(product_id)
            
            # Find category (simplified - in real app would query product database)
            if product_id in self.product_knowledge:
                category = self.product_knowledge[product_id]["category"]
                categories_in_cart.add(category)
        
        # Generate different recommendation strategies
        if event_type == "add":
            recommendations.extend(self._get_complement_recommendations(products_in_cart))
        elif event_type == "checkout":
            recommendations.extend(self._get_post_purchase_recommendations(products_in_cart))
        else:
            recommendations.extend(self._get_general_recommendations(products_in_cart, categories_in_cart))
        
        # Simulate AI reasoning with category trends
        recommendations.extend(self._get_trend_based_recommendations(categories_in_cart))
        
        # Limit to top 5 recommendations and add AI reasoning
        return self._rank_and_limit_recommendations(recommendations)
    
    def _get_complement_recommendations(self, products_in_cart: List[str]) -> List[Dict[str, Any]]:
        """Get products that complement items in cart"""
        recommendations = []
        
        for product_id in products_in_cart:
            if product_id in self.product_knowledge:
                complements = self.product_knowledge[product_id]["complements"]
                for complement in complements[:2]:  # Limit to 2 per product
                    recommendations.append({
                        "name": complement.title(),
                        "reason": f"Complements your {self._get_product_name(product_id)}",
                        "price": self._estimate_price(complement),
                        "confidence": 0.8,
                        "type": "complement"
                    })
        
        return recommendations
    
    def _get_post_purchase_recommendations(self, products_in_cart: List[str]) -> List[Dict[str, Any]]:
        """Get recommendations for post-purchase (accessories, warranties, etc.)"""
        recommendations = []
        
        for product_id in products_in_cart:
            if product_id in self.product_knowledge:
                accessories = self.product_knowledge[product_id]["accessories"]
                for accessory in accessories[:2]:
                    recommendations.append({
                        "name": accessory.title(),
                        "reason": f"Protect and enhance your new {self._get_product_name(product_id)}",
                        "price": self._estimate_price(accessory),
                        "confidence": 0.7,
                        "type": "accessory"
                    })
        
        return recommendations
    
    def _get_general_recommendations(self, products_in_cart: List[str], 
                                   categories_in_cart: set) -> List[Dict[str, Any]]:
        """Get general recommendations based on cart analysis"""
        recommendations = []
        
        # Recommend upgrades
        for product_id in products_in_cart:
            if product_id in self.product_knowledge:
                upgrades = self.product_knowledge[product_id]["upgrades"]
                if upgrades:
                    recommendations.append({
                        "name": upgrades[0].title(),
                        "reason": f"Upgrade from your current selection",
                        "price": self._estimate_price(upgrades[0], premium=True),
                        "confidence": 0.6,
                        "type": "upgrade"
                    })
        
        return recommendations
    
    def _get_trend_based_recommendations(self, categories_in_cart: set) -> List[Dict[str, Any]]:
        """Get recommendations based on current trends in categories"""
        recommendations = []
        
        for category in categories_in_cart:
            if category in self.category_trends:
                trends = self.category_trends[category]
                trend = trends[0]  # Use first trend
                recommendations.append({
                    "name": f"{trend.title()} {category} Item",
                    "reason": f"Trending in {category}: {trend} products",
                    "price": self._estimate_price(f"{trend} item"),
                    "confidence": 0.5,
                    "type": "trending"
                })
        
        return recommendations
    
    def _get_popular_recommendations(self) -> List[Dict[str, Any]]:
        """Get popular recommendations when cart is empty"""
        return [
            {
                "name": "Wireless Bluetooth Earbuds",
                "reason": "Most popular item this week",
                "price": 79.99,
                "confidence": 0.9,
                "type": "popular"
            },
            {
                "name": "Portable Phone Charger",
                "reason": "Essential for mobile devices",
                "price": 24.99,
                "confidence": 0.8,
                "type": "popular"
            },
            {
                "name": "Blue Light Blocking Glasses", 
                "reason": "Great for computer users",
                "price": 34.99,
                "confidence": 0.7,
                "type": "popular"
            }
        ]
    
    def _rank_and_limit_recommendations(self, recommendations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Rank recommendations by confidence and limit to top 5"""
        # Sort by confidence descending
        sorted_recs = sorted(recommendations, key=lambda x: x.get("confidence", 0), reverse=True)
        
        # Remove duplicates and limit to 5
        seen_names = set()
        unique_recs = []
        
        for rec in sorted_recs:
            name = rec.get("name", "")
            if name not in seen_names and len(unique_recs) < 5:
                seen_names.add(name)
                unique_recs.append(rec)
        
        return unique_recs
    
    def _get_product_name(self, product_id: str) -> str:
        """Get human-readable product name from ID"""
        name_mapping = {
            "laptop-001": "laptop",
            "phone-001": "smartphone", 
            "headphones-001": "headphones",
            "book-001": "programming book",
            "coffee-001": "coffee",
            "watch-001": "smartwatch"
        }
        return name_mapping.get(product_id, "product")
    
    def _estimate_price(self, item_name: str, premium: bool = False) -> float:
        """Estimate price for recommended item"""
        base_prices = {
            "mouse": 25.99,
            "laptop bag": 49.99,
            "external monitor": 199.99,
            "wireless keyboard": 79.99,
            "phone case": 19.99,
            "screen protector": 12.99,
            "wireless charger": 29.99,
            "power bank": 34.99,
            "headphone stand": 24.99,
            "audio cable": 15.99,
            "notebook": 8.99,
            "highlighters": 6.99,
            "coffee grinder": 89.99,
            "french press": 29.99,
            "watch band": 39.99,
            "charging dock": 49.99
        }
        
        # Find closest match
        for key, price in base_prices.items():
            if key in item_name.lower():
                return price * 1.3 if premium else price
        
        # Default estimation
        return 49.99 if premium else 24.99

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
    parser = argparse.ArgumentParser(description="E-commerce Recommendations Flink Job")
    parser.add_argument('--bootstrap-servers', default=DEFAULT_BOOTSTRAP_SERVERS,
                       help=f'Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP_SERVERS})')
    parser.add_argument('--input-topic', default=DEFAULT_INPUT_TOPIC,
                       help=f'Input Kafka topic for basket events (default: {DEFAULT_INPUT_TOPIC})')
    parser.add_argument('--output-topic', default=DEFAULT_OUTPUT_TOPIC,
                       help=f'Output Kafka topic for recommendations (default: {DEFAULT_OUTPUT_TOPIC})')
    parser.add_argument('--consumer-group', default=DEFAULT_CONSUMER_GROUP,
                       help=f'Kafka consumer group ID (default: {DEFAULT_CONSUMER_GROUP})')
    
    args = parser.parse_args()
    
    logger.info(f"Starting E-commerce Recommendations job")
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
    
    # Create data stream and process
    logger.info("Setting up data stream...")
    recommendations = (
        env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "basket_events_source")
        .map(RecommendationEngine())
    )
    
    # Send recommendations to output topic
    recommendations.sink_to(kafka_sink)
    
    logger.info("Starting job execution...")
    
    # Execute the job
    try:
        env.execute("E-commerce Recommendations")
    except Exception as e:
        logger.error(f"Job execution failed: {e}")
        logger.info("Tip: Try using the standalone service instead:")
        logger.info("  python3 kafka_recommendations_standalone.py")
        raise

if __name__ == "__main__":
    main() 