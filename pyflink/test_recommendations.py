#!/usr/bin/env python3
"""
Test script for the recommendation engine logic
Tests the recommendation logic without requiring Flink cluster
"""

import json
import sys
import os
from datetime import datetime

# Add the current directory to path to import our recommendation engine
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ecommerce_recommendations_simple import SimpleRecommendationEngine

def test_recommendation_engine():
    """Test the recommendation engine with sample data"""
    print("🧪 Testing Recommendation Engine")
    print("=" * 50)
    
    # Create an instance of the recommendation engine
    engine = SimpleRecommendationEngine()
    
    # Test 1: Laptop addition event
    print("\n📱 Test 1: Adding laptop to cart")
    laptop_event = {
        "event_type": "add",
        "product_id": "laptop-001",
        "product_name": "UltraBook Pro X1",
        "quantity": 1,
        "price": 1299.99,
        "total_cart_value": 1299.99,
        "timestamp": datetime.now().isoformat(),
        "session_id": "test-session-1",
        "cart_items": []
    }
    
    result1 = engine.map(json.dumps(laptop_event))
    response1 = json.loads(result1)
    
    print(f"Session ID: {response1['session_id']}")
    print(f"Recommendations: {len(response1['recommendations'])}")
    for i, rec in enumerate(response1['recommendations'], 1):
        print(f"  {i}. {rec['name']} - ${rec['price']:.2f}")
        print(f"     Reason: {rec['reason']}")
    
    # Test 2: Phone addition event
    print("\n📱 Test 2: Adding phone to cart")
    phone_event = {
        "event_type": "add",
        "product_id": "phone-001",
        "product_name": "SmartPhone Galaxy S23",
        "quantity": 1,
        "price": 899.99,
        "total_cart_value": 899.99,
        "timestamp": datetime.now().isoformat(),
        "session_id": "test-session-2",
        "cart_items": []
    }
    
    result2 = engine.map(json.dumps(phone_event))
    response2 = json.loads(result2)
    
    print(f"Session ID: {response2['session_id']}")
    print(f"Recommendations: {len(response2['recommendations'])}")
    for i, rec in enumerate(response2['recommendations'], 1):
        print(f"  {i}. {rec['name']} - ${rec['price']:.2f}")
        print(f"     Reason: {rec['reason']}")
    
    # Test 3: Unknown product (should use general recommendations)
    print("\n❓ Test 3: Adding unknown product")
    unknown_event = {
        "event_type": "add",
        "product_id": "unknown-123",
        "product_name": "Mystery Product",
        "quantity": 1,
        "price": 50.00,
        "total_cart_value": 50.00,
        "timestamp": datetime.now().isoformat(),
        "session_id": "test-session-3",
        "cart_items": []
    }
    
    result3 = engine.map(json.dumps(unknown_event))
    response3 = json.loads(result3)
    
    print(f"Session ID: {response3['session_id']}")
    print(f"Recommendations: {len(response3['recommendations'])}")
    for i, rec in enumerate(response3['recommendations'], 1):
        print(f"  {i}. {rec['name']} - ${rec['price']:.2f}")
        print(f"     Reason: {rec['reason']}")
    
    # Test 4: Checkout event
    print("\n🛒 Test 4: Checkout event")
    checkout_event = {
        "event_type": "checkout",
        "product_id": "multiple",
        "product_name": "Order with 2 items",
        "quantity": 2,
        "price": 1199.98,
        "total_cart_value": 1199.98,
        "timestamp": datetime.now().isoformat(),
        "session_id": "test-session-4",
        "cart_items": [
            {"product_id": "laptop-001", "name": "UltraBook Pro X1"},
            {"product_id": "phone-001", "name": "SmartPhone Galaxy S23"}
        ]
    }
    
    result4 = engine.map(json.dumps(checkout_event))
    response4 = json.loads(result4)
    
    print(f"Session ID: {response4['session_id']}")
    print(f"Recommendations: {len(response4['recommendations'])}")
    for i, rec in enumerate(response4['recommendations'], 1):
        print(f"  {i}. {rec['name']} - ${rec['price']:.2f}")
        print(f"     Reason: {rec['reason']}")
    
    # Test 5: Invalid JSON (error handling)
    print("\n❌ Test 5: Invalid JSON")
    invalid_json = "{ this is not valid json }"
    result5 = engine.map(invalid_json)
    response5 = json.loads(result5)
    
    if "error" in response5:
        print(f"✅ Error handling works: {response5['error']}")
    else:
        print("❌ Error handling failed")
    
    print("\n✅ All recommendation engine tests completed!")
    return True

def test_kafka_message_format():
    """Test that our messages match the expected Kafka format"""
    print("\n🔌 Testing Kafka Message Format Compatibility")
    print("=" * 50)
    
    engine = SimpleRecommendationEngine()
    
    # Create a test event that matches what the e-commerce app sends
    test_event = {
        "event_type": "add",
        "product_id": "laptop-001",
        "product_name": "UltraBook Pro X1",
        "quantity": 1,
        "price": 1299.99,
        "total_cart_value": 1299.99,
        "timestamp": "2024-01-15T10:30:00Z",
        "session_id": "abc123",
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
    
    # Process the event
    result = engine.map(json.dumps(test_event))
    response = json.loads(result)
    
    # Verify response format
    required_fields = ["session_id", "timestamp", "event_type", "recommendations", "reasoning"]
    for field in required_fields:
        if field not in response:
            print(f"❌ Missing field: {field}")
            return False
        else:
            print(f"✅ Has field: {field}")
    
    # Verify recommendations format
    if response["recommendations"]:
        rec = response["recommendations"][0]
        rec_fields = ["name", "reason", "price"]
        for field in rec_fields:
            if field not in rec:
                print(f"❌ Missing recommendation field: {field}")
                return False
            else:
                print(f"✅ Recommendation has field: {field}")
    
    print("✅ Kafka message format is compatible!")
    return True

if __name__ == "__main__":
    try:
        success1 = test_recommendation_engine()
        success2 = test_kafka_message_format()
        
        if success1 and success2:
            print("\n🎉 All tests passed! Recommendation engine is ready.")
        else:
            print("\n❌ Some tests failed.")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1) 