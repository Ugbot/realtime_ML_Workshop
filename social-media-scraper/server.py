import pprint
from typing import Dict, List, Optional
from datetime import datetime

from aiokafka.helpers import create_ssl_context
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from aiokafka import AIOKafkaConsumer
import asyncio
from collections import deque

from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import StreamingResponse, HTMLResponse
from sse_starlette import EventSourceResponse
import mastodon_types
import json
import aiofiles

app = FastAPI()
events = []


class RingBuffer:
    def __init__(self, size):
        self.data = [None for _ in range(size)]
        self.size = size
        self.index = 0

    def append(self, item):
        self.data[self.index] = item
        self.index = (self.index + 1) % self.size

    def get(self):
        if self.index == 0:
            return self.data
        else:
            return self.data[self.index:] + self.data[:self.index]


context = create_ssl_context(
    cafile="demo_keys/ca.pem",
    certfile="demo_keys/service.cert",
    keyfile="demo_keys/service.key",
)
kafka_bootstrap_servers = "zilla-kafka-devrel-ben.aivencloud.com:15545"

# kafka_topic = "complete-json"
kafka_topic = "easy-timestamps"

# MastodonAccount and MastodonPost classes have been defined above

# A deque is a good data structure for this, as it allows for efficient appending and popping from both ends
post_cache = deque(maxlen=50)

origins = [
    "http://localhost:8000",  # Adjust this to match the origin of your client application
    # "http://your-other-domain.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    # allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def kafka_consumer():
    consumer = AIOKafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        security_protocol='SSL',
        ssl_context=context,
        auto_offset_reset="earliest",
        #    group_id="my-group"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            post_data = json.loads(msg.value)
            # mastodon_post = mastodon_types.MastodonPost(**post_data)

            post_cache.append(post_data)
            print(f"Received message")
    finally:
        await consumer.stop()


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(kafka_consumer())


@app.on_event("shutdown")
async def shutdown_event():
    if kafka_consumer is not None:
        await kafka_consumer.stop()


connected_clients = []


@app.get("/sse")
async def get_events(request: Request):
    async def event_generator():
        try:
            while True:
                while len(post_cache) > 0:
                    await asyncio.sleep(0.2)  # Wait a bit before checking for new posts
                    post = post_cache.popleft()
                    # pprint.pprint(post)
                    yield f"{json.dumps(post)}\n\n"
        except asyncio.CancelledError:
            # the client disconnected, so we need to stop
            connected_clients.remove(request)
            raise HTTPException(status_code=204) from None

    connected_clients.append(request)
    return EventSourceResponse(event_generator())


@app.get("/", response_class=HTMLResponse)
async def get_root():
    async with aiofiles.open("index.html", mode='r') as f:
        content = await f.read()
    return HTMLResponse(content=content)


@app.get("/testsse")
async def get_test_events():
    async def event_generator():
        i = 0
        while True:
            yield f"data: {i}\n\n"
            i += 1
            await asyncio.sleep(1)

    return EventSourceResponse(event_generator())
