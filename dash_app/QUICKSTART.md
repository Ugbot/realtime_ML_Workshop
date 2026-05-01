# Quick Start Guide - E-commerce with Kafka Recommendations

Get the Kafka-enabled e-commerce system running in under 5 minutes!

## 🚀 Quick Setup (3 Steps)

### 1. Install Dependencies
```bash
cd dash_app
pip install -r requirements.txt
```

### 2. Start the E-commerce App
```bash
python3 ecommerce_app.py
```

### 3. Start the Recommendation Service

Choose one of these working options:

**Option A: Standalone Service (Easiest)**
```bash
cd ../pyflink
python3 kafka_recommendations_standalone.py
```

**Option B: Simple Flink Job**
```bash
cd ../pyflink
python3 ecommerce_recommendations_simple.py
```

**Option C: Advanced Flink Job (Full AI Features)**
```bash
cd ../pyflink
python3 ecommerce_recommendations.py
```

All services now use sensible defaults and require no arguments! The JSON handling issues have been fixed.

### 4. Open the App
Visit: http://localhost:8050

## ✅ What You'll See

1. **E-commerce Website**: Modern product catalog with cart functionality
2. **Real-time Recommendations**: AI suggestions that update as you shop
3. **Kafka Integration**: Behind-the-scenes event streaming

## 🧪 Test It Out

1. **Add a laptop to cart** → See mouse, laptop bag, monitor recommendations
2. **Add a phone to cart** → See case, charger, earbuds recommendations
3. **Remove items** → Watch recommendations update in real-time
4. **Checkout** → See post-purchase recommendations

## 📊 Monitor the System

### View Kafka Messages
```bash
# Watch basket events being published
kafka-console-consumer --bootstrap-server localhost:19092 --topic basket-events

# Watch recommendations being generated
kafka-console-consumer --bootstrap-server localhost:19092 --topic recommendations
```

### Check Logs
- **E-commerce App**: Look for `[Kafka]` messages in console
- **Recommendation Service**: See processing logs in real-time

## 🔧 Troubleshooting

### Kafka Not Running?
If you see connection errors, make sure Kafka/Redpanda is running on port 19092.

### Flink Job Crashing?
The standalone service (`kafka_recommendations_standalone.py`) is more reliable than the Flink version.

### No Recommendations?
1. Check both services are running
2. Look for error messages in console
3. Try the test script: `python3 test_recommendations.py`

## 📁 Key Files

- `dash_app/ecommerce_app.py` - Main e-commerce application
- `pyflink/kafka_recommendations_standalone.py` - Recommendation service
- `pyflink/test_recommendations.py` - Test the recommendation logic
- `README_kafka_integration.md` - Detailed documentation

## ⚡ Next Steps

1. **Customize Products**: Edit the product catalog in `ecommerce_app.py`
2. **Improve Recommendations**: Modify the logic in the recommendation service
3. **Add Real LLM**: Replace the simulation with actual AI API calls
4. **Scale Up**: Run multiple recommendation service instances

## 🎯 Architecture

```
[User Shopping] → [Dash App] → [Kafka: basket-events] → [Recommendation Service]
                                                              ↓
[Updated UI] ← [Dash App] ← [Kafka: recommendations] ← [AI Logic]
```

The system demonstrates real-time ML/AI integration with streaming data - perfect for learning modern data architecture patterns! 