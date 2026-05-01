# E-commerce Kafka Integration

This document describes the Kafka integration in the e-commerce application, which enables real-time basket event streaming and AI-powered recommendations.

## Overview

The e-commerce application now includes:
- **Kafka Producer**: Publishes basket events (add, remove, update, checkout) to the `basket-events` topic
- **Kafka Consumer**: Listens for AI recommendations on the `recommendations` topic
- **Recommendation Services**: Two implementations available:
  - **Standalone Service**: Simple aiokafka-based service (recommended for testing)
  - **Flink Job**: Advanced PyFlink implementation (requires Flink cluster)

## Kafka Topics

### 1. basket-events
**Producer**: E-commerce Dashboard  
**Consumer**: Recommendation Engine  
**Purpose**: Stream all basket/cart interactions

**Message Format**:
```json
{
  "event_type": "add|remove|update|checkout",
  "product_id": "product-id",
  "product_name": "Product Name",
  "quantity": 2,
  "price": 99.99,
  "total_cart_value": 199.98,
  "timestamp": "2024-01-15T10:30:00Z",
  "session_id": "unique-session-id",
  "cart_items": [
    {
      "product_id": "product-id",
      "name": "Product Name",
      "price": 99.99,
      "quantity": 2,
      "total": 199.98,
      "image_url": "https://..."
    }
  ]
}
```

### 2. recommendations
**Producer**: Recommendation Engine  
**Consumer**: E-commerce Dashboard  
**Purpose**: Stream AI-generated product recommendations

**Message Format**:
```json
{
  "session_id": "unique-session-id",
  "timestamp": "2024-01-15T10:30:05Z",
  "event_type": "add",
  "recommendations": [
    {
      "name": "Wireless Mouse",
      "reason": "Complements your laptop",
      "price": 25.99,
      "confidence": 0.8,
      "type": "complement"
    }
  ],
  "reasoning": "AI-generated based on cart contents and purchase patterns"
}
```

## Configuration

### Kafka Connection Settings
```python
KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
KAFKA_BASKET_TOPIC = "basket-events"
KAFKA_RECOMMENDATIONS_TOPIC = "recommendations"
KAFKA_GROUP_ID = "ecommerce-dashboard"
```

These settings match the project's existing Kafka configuration used in other components.

## Running the Complete System

### 1. Start Kafka/Redpanda
Make sure Kafka is running on `localhost:19092`. The project uses Redpanda as the Kafka implementation.

### 2. Start the E-commerce App
```bash
cd dash_app
python3 ecommerce_app.py
```

The app automatically starts:
- Kafka producer in a background thread
- Kafka consumer in a background thread
- Web server on port 8050

### 3. Start a Recommendation Service

#### Option A: Standalone Service (Recommended for Development)
```bash
cd pyflink
python3 kafka_recommendations_standalone.py
```

This is the simpler, more reliable option that uses aiokafka directly.

#### Option B: Simple Flink Job 
```bash
cd pyflink
python3 ecommerce_recommendations_simple.py
```

This requires a Flink cluster to be running. The job now includes hardcoded defaults from `hello_kafka.py`, so no arguments are required. If you get Java errors, use Option A instead.

#### Option C: Advanced Flink Job (Complex AI Logic)
```bash
cd pyflink
python3 ecommerce_recommendations.py
```

This is the full-featured version with sophisticated recommendation strategies, confidence scoring, and trend analysis. Requires Flink cluster.

You can still customize any Flink job with arguments if needed:
```bash
python3 ecommerce_recommendations.py \
  --bootstrap-servers localhost:19092 \
  --input-topic basket-events \
  --output-topic recommendations
```

### 4. Monitor Topics (Optional)
Use the project's existing Kafka tools or consumer scripts to monitor messages:

**Monitor basket events**:
```bash
kafka-console-consumer --bootstrap-server localhost:19092 --topic basket-events --from-beginning
```

**Monitor recommendations**:
```bash
kafka-console-consumer --bootstrap-server localhost:19092 --topic recommendations --from-beginning
```

## Testing

### 1. Unit Tests
Test the e-commerce store functionality:
```bash
cd dash_app
python3 test_ecommerce.py
```

### 2. Recommendation Logic Tests
Test the recommendation engine logic:
```bash
cd pyflink
python3 test_recommendations.py
```

This tests the recommendation algorithms without requiring Kafka.

### 3. Integration Testing
1. Start the e-commerce app: `python3 ecommerce_app.py`
2. Start the recommendation service: `python3 kafka_recommendations_standalone.py`
3. Add products to cart in the web UI at `http://localhost:8050`
4. Observe real-time recommendations appearing in the recommendations panel

### 4. Manual Kafka Testing

**Test basket event publishing**:
```bash
echo '{"event_type":"add","product_id":"laptop-001","product_name":"Test Laptop","quantity":1,"price":999.99,"total_cart_value":999.99,"timestamp":"'$(date -Iseconds)'","session_id":"test-session","cart_items":[]}' | \
kafka-console-producer --broker-list localhost:19092 --topic basket-events
```

**Test recommendation consumption**:
```bash
echo '{"session_id":"test-session","timestamp":"'$(date -Iseconds)'","recommendations":[{"name":"Test Mouse","reason":"Goes well with laptops","price":25.99}]}' | \
kafka-console-producer --broker-list localhost:19092 --topic recommendations
```

## Available Services

### Advanced Flink Recommendation Job
- **File**: `pyflink/ecommerce_recommendations.py`
- **Dependencies**: PyFlink, Flink cluster
- **Features**: Complex AI logic, multiple recommendation strategies, confidence scoring
- **Pros**: Advanced features, sophisticated recommendation logic
- **Cons**: Requires Flink cluster setup
- **Recommended for**: Production with Flink infrastructure, advanced use cases

### Simple Flink Recommendation Job
- **File**: `pyflink/ecommerce_recommendations_simple.py`
- **Dependencies**: PyFlink, Flink cluster
- **Features**: Basic recommendation logic
- **Pros**: Simpler code, easier to understand
- **Cons**: Requires Flink cluster setup
- **Recommended for**: Learning Flink, simple deployments

### Standalone Recommendation Service
- **File**: `pyflink/kafka_recommendations_standalone.py`
- **Dependencies**: Only aiokafka
- **Pros**: Simple, reliable, easy to debug
- **Cons**: Not using Flink ecosystem
- **Recommended for**: Development, testing, simple deployments

All services now use the same working JSON handling approach from `topic_splitter.py`.

## Features

### Basket Event Streaming
Every cart interaction triggers a Kafka message:
- **Add to Cart**: Publishes product details and current cart state
- **Update Quantity**: Publishes quantity changes
- **Remove Item**: Publishes removal events
- **Checkout**: Publishes complete order information

### Real-time Recommendations
The recommendation engine processes basket events in real-time and generates:
- **Complement Products**: Items that work well together
- **Accessories**: Protective/enhancement items
- **Upgrades**: Premium alternatives
- **Popular Items**: Best-selling products

### Smart Recommendation Logic
Both services include:
- Product knowledge base mapping
- Event-specific reasoning
- Fallback to general recommendations
- Error handling and recovery

## Monitoring and Observability

### Application Logs
The e-commerce app logs all Kafka operations:
- `[Kafka] Producer started`
- `[Kafka] Consumer started`
- `[Kafka] Published basket event: add - Product Name`
- `[Kafka] Received recommendation: {...}`

### Recommendation Service Logs
Both services log processing activities:
- `Processing add event for Product Name (ID: product-001)`
- `Sent 3 recommendations for session xyz`
- `Started Kafka consumer for topic: basket-events`

### Session Tracking
Each user session gets a unique ID that's included in all events, enabling:
- Session-specific recommendations
- User journey tracking
- Recommendation effectiveness analysis

## Troubleshooting

**UPDATE**: Major Flink/JSON issues have been resolved! All three recommendation services now work properly. ✅

### Common Issues

#### ~~Flink Job Crashes~~ (FIXED ✅)
**Problem**: `java.lang.IllegalArgumentException: Only RowTypeInfo is supported`  
**Solution**: All Flink jobs now use the working `SimpleStringSchema` approach from `topic_splitter.py`

#### ~~ClassCastException in Flink~~ (FIXED ✅)
**Problem**: `class [B cannot be cast to class java.lang.String`  
**Solution**: Updated all Flink jobs to use modern `KafkaSource`/`KafkaSink` instead of deprecated connectors

#### Kafka Connection Errors
**Problem**: `Failed to connect to Kafka`
**Solutions**:
1. Verify Kafka is running: `netstat -an | grep 19092`
2. Check bootstrap servers setting
3. Verify topic exists: `kafka-topics --list --bootstrap-server localhost:19092`

#### No Recommendations Appearing
**Solutions**:
1. Check if recommendation service is running
2. Verify topics are being written to/read from
3. Check consumer group is consuming messages
4. Look for errors in application logs

All Flink jobs now use the same reliable JSON handling and Kafka connector approach.

### Debug Commands

**List Kafka topics**:
```bash
kafka-topics --list --bootstrap-server localhost:19092
```

**Check consumer group status**:
```bash
kafka-consumer-groups --bootstrap-server localhost:19092 --group ecommerce-dashboard --describe
```

**Monitor logs**:
```bash
# E-commerce app logs
tail -f ecommerce_app.log

# Recommendation service logs  
tail -f recommendation_service.log
```

## Production Considerations

### Scaling
- **Multiple App Instances**: Each instance gets a unique session ID
- **Recommendation Service**: Run multiple instances with same consumer group
- **Kafka Partitioning**: Use product categories or user IDs as partition keys

### Reliability
- **Error Handling**: All Kafka operations include try/catch blocks
- **Graceful Degradation**: App works without Kafka connectivity
- **Message Durability**: Kafka provides at-least-once delivery
- **Health Checks**: Services include connection monitoring

### Performance
- **Batch Processing**: Services can be configured for batch recommendations
- **Caching**: Add Redis for recommendation caching
- **Load Balancing**: Multiple recommendation service instances

### Real LLM Integration
Replace the simulated recommendation engine with actual LLM calls:

**OpenAI Integration**:
```python
import openai

async def generate_llm_recommendations(basket_data):
    prompt = f"Generate 3 product recommendations for a customer who just added {basket_data['product_name']} to their cart."
    response = await openai.ChatCompletion.acreate(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}]
    )
    return parse_recommendations(response.choices[0].message.content)
```

**Local LLM Integration**:
```python
import requests

async def call_local_llm(basket_data):
    response = requests.post('http://localhost:8000/recommend', 
                           json=basket_data)
    return response.json()['recommendations']
```

## Future Enhancements

- **User Authentication**: Persistent user IDs for better recommendations
- **A/B Testing**: Different recommendation strategies
- **Metrics Collection**: Click-through rates, conversion tracking
- **Real-time Model Updates**: Dynamic recommendation model updates
- **Multi-language Support**: Localized recommendations
- **Inventory Integration**: Stock-aware recommendations
- **Machine Learning Pipeline**: Training models from user behavior data 