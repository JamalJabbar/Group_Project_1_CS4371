import argparse
import asyncio
import json
import logging
import os
import random
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import websockets
from websockets.exceptions import ConnectionClosed

DEFAULT_URL = (
    "wss://jetstream.us-east.bsky.network/xrpc/"
    "network.bsky.jetstream.subscribeEvents?collections=app.bsky.feed.post&kinds=commit"
)
LOG = logging.getLogger("jetstream")


def make_url(url, cursor):
    if cursor is None:
        return url
    parts = urlsplit(url)
    query = [(k, v) for k, v in parse_qsl(parts.query) if k != "cursor"]
    query.append(("cursor", str(cursor)))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def is_english_post(event):
    payload = event.get("payload", {})
    record = payload.get("record") or {}
    text = record.get("text")
    return (payload.get("collection") == "app.bsky.feed.post"
            and payload.get("operation") == "create"
            and isinstance(text, str) and bool(text.strip())
            and "en" in (record.get("langs") or []))


def create_topic(bootstrap_servers, topic):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, request_timeout_ms=30000)
    try:
        try:
            admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
            LOG.info("Created topic %s", topic)
        except TopicAlreadyExistsError:
            LOG.info("Using existing topic %s", topic)
    finally:
        admin.close()


async def stream(args):
    create_topic(args.bootstrap_servers, args.topic)
    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers, acks="all", retries=5)
    sent = 0
    cursor = None
    try:
        while not args.max_events or sent < args.max_events:
            try:
                async with websockets.connect(
                    make_url(args.url, cursor), subprotocols=["xrpc.v1.json"],
                    open_timeout=20, ping_interval=20, ping_timeout=20,
                ) as websocket:
                    LOG.info("Connected to Jetstream")
                    async for frame in websocket:
                        event = json.loads(frame)
                        if not is_english_post(event) or random.random() >= args.sample_rate:
                            continue
                        data = json.dumps(event, ensure_ascii=False).encode("utf-8")
                        await asyncio.to_thread(producer.send(args.topic, value=data).get, timeout=30)
                        cursor = event["payload"].get("seq", cursor)
                        sent += 1
                        if sent % 100 == 0 or sent == args.max_events:
                            LOG.info("Stored %d posts in %s", sent, args.topic)
                        if args.max_events and sent >= args.max_events:
                            return
            except (OSError, ConnectionClosed, TimeoutError) as error:
                LOG.warning("Jetstream connection interrupted: %s; retrying in 5 seconds", error)
                await asyncio.sleep(5)
    finally:
        producer.close(timeout=30)


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Stream English Bluesky posts to Kafka.")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default="topic1")
    parser.add_argument("--url", default=DEFAULT_URL, help="Jetstream v2 subscription URL")
    parser.add_argument("--max-events", type=int, default=0, help="stop after this many posts; 0 means continuous")
    parser.add_argument("--sample-rate", type=float, default=1.0, help="fraction of English posts to keep")
    args = parser.parse_args()
    if args.max_events < 0:
        parser.error("--max-events must be nonnegative")
    if not 0 < args.sample_rate <= 1:
        parser.error("--sample-rate must be greater than 0 and at most 1")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)
    try:
        asyncio.run(stream(args))
    except KeyboardInterrupt:
        LOG.info("Stopped by user")


if __name__ == "__main__":
    main()
