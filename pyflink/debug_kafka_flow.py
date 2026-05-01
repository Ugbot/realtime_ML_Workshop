#!/usr/bin/env python3
"""
Debug script to test Kafka message flow for e-commerce recommendations

This script helps debug why the frontend isn't showing recommendations by:
1. Testing if basket events are being published
2. Testing if recommendations are being consumed
3. Verifying message formats match
"""

import asyncio
import json
import logging
from datetime import datetime
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
BASKET_TOPIC = "basket-events-v2"
RECOMMENDATIONS_TOPIC = "recommendations-v2"

async def test_message_flow():
    """Test the complete message flow"""
    
    print("🔍 Testing Kafka Message Flow for E-commerce Recommendations")
    print("=" * 60)
    
    # Test 1: Check if we can connect to Kafka
    print("\n1. Testing Kafka Connection...")
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        await producer.start()
        print("✅ Kafka connection successful")
        await producer.stop()
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        return
    
    # Test 2: Create a test basket event and publish it
    print("\n2. Publishing Test Basket Event...")
    test_basket_event = {
        "event_type": "add",
        "product_id": "laptop-001",
        "product_name": "UltraBook Pro X1",
        "quantity": 1,
        "price": 1299.99,
        "total_cart_value": 1299.99,
        "timestamp": datetime.now().isoformat(),
        "session_id": "debug-test-session",
        "cart_items": [
            {
                "product_id": "laptop-001",
                "name": "UltraBook Pro X1",
                "price": 1299.99,
                "quantity": 1,
                "total": 1299.99,
                "image_url": "https://example.com/laptop.jpg"
            }
        ]
    }
    
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    await producer.start()
    
    try:
        await producer.send_and_wait(BASKET_TOPIC, test_basket_event)
        print(f"✅ Published test basket event to {BASKET_TOPIC}")
        print(f"   Event: {test_basket_event['event_type']} - {test_basket_event['product_name']}")
    except Exception as e:
        print(f"❌ Failed to publish basket event: {e}")
    finally:
        await producer.stop()
    
    # Test 3: Listen for recommendations
    print(f"\n3. Listening for Recommendations on {RECOMMENDATIONS_TOPIC}...")
    print("   (Will wait 10 seconds for recommendation service to respond)")
    
    consumer = AIOKafkaConsumer(
        RECOMMENDATIONS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="debug-consumer",
        value_deserializer=lambda m: m.decode("utf-8"),
        auto_offset_reset="latest"
    )
    
    await consumer.start()
    
    try:
        # Wait for recommendations with timeout
        timeout_task = asyncio.create_task(asyncio.sleep(10))
        message_task = asyncio.create_task(consumer.__anext__())
        
        done, pending = await asyncio.wait(
            [timeout_task, message_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Cancel pending tasks
        for task in pending:
            task.cancel()
        
        if timeout_task in done:
            print("❌ No recommendations received within 10 seconds")
            print("   Possible issues:")
            print("   - Recommendation service not running")
            print("   - Service not processing messages")
            print("   - Wrong topic names")
        else:
            # We got a message
            msg = message_task.result()
            print("✅ Received recommendation!")
            try:
                recommendation = json.loads(msg.value)
                print(f"   Session ID: {recommendation.get('session_id', 'N/A')}")
                print(f"   Recommendations: {len(recommendation.get('recommendations', []))}")
                for i, rec in enumerate(recommendation.get('recommendations', []), 1):
                    print(f"     {i}. {rec.get('name', 'Unknown')} - ${rec.get('price', 0):.2f}")
                    print(f"        Reason: {rec.get('reason', 'No reason')}")
            except json.JSONDecodeError as e:
                print(f"❌ Failed to parse recommendation JSON: {e}")
                print(f"   Raw message: {msg.value}")
                
    except Exception as e:
        print(f"❌ Error listening for recommendations: {e}")
    finally:
        await consumer.stop()
    
    # Test 4: Check message format compatibility
    print(f"\n4. Testing Message Format Compatibility...")
    
    # Test what the e-commerce app expects
    expected_format = {
        "session_id": "string",
        "timestamp": "ISO datetime string", 
        "recommendations": [
            {
                "name": "string",
                "reason": "string", 
                "price": "number"
            }
        ]
    }
    
    print("✅ Expected recommendation format:")
    print(json.dumps(expected_format, indent=2))

async def monitor_topics():
    """Monitor both topics for activity"""
    
    print("\n🔍 Monitoring Kafka Topics (Press Ctrl+C to stop)")
    print("=" * 50)
    
    # Monitor basket events
    basket_consumer = AIOKafkaConsumer(
        BASKET_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="debug-basket-monitor",
        value_deserializer=lambda m: m.decode("utf-8"),
        auto_offset_reset="latest"
    )
    
    # Monitor recommendations  
    rec_consumer = AIOKafkaConsumer(
        RECOMMENDATIONS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="debug-rec-monitor",
        value_deserializer=lambda m: m.decode("utf-8"),
        auto_offset_reset="latest"
    )
    
    await basket_consumer.start()
    await rec_consumer.start()
    
    try:
        while True:
            # Check for basket events
            try:
                msg = await asyncio.wait_for(basket_consumer.getone(), timeout=0.1)
                event = json.loads(msg.value)
                print(f"📦 BASKET EVENT: {event.get('event_type')} - {event.get('product_name')} (session: {event.get('session_id', 'N/A')[:8]}...)")
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                print(f"❌ Error reading basket event: {e}")
            
            # Check for recommendations
            try:
                msg = await asyncio.wait_for(rec_consumer.getone(), timeout=0.1)
                rec = json.loads(msg.value)
                rec_count = len(rec.get('recommendations', []))
                print(f"🤖 RECOMMENDATION: {rec_count} items for session {rec.get('session_id', 'N/A')[:8]}...")
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                print(f"❌ Error reading recommendation: {e}")
                
            await asyncio.sleep(0.5)
            
    except KeyboardInterrupt:
        print("\n👋 Monitoring stopped")
    finally:
        await basket_consumer.stop()
        await rec_consumer.stop()

async def main():
    """Main debug function"""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "monitor":
        await monitor_topics()
    else:
        await test_message_flow()
        
        print(f"\n💡 Next Steps:")
        print(f"1. Make sure recommendation service is running:")
        print(f"   python3 kafka_recommendations_standalone.py")
        print(f"2. Make sure e-commerce app is running:")
        print(f"   cd ../dash_app && python3 ecommerce_app.py")
        print(f"3. Monitor live traffic:")
        print(f"   python3 debug_kafka_flow.py monitor")

if __name__ == "__main__":
    asyncio.run(main()) 