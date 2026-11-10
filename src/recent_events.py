import argparse
import json
import os
import time

from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Print recent Kafka records.")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default="topic1")
    parser.add_argument("--count", type=int, default=10)
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be positive")

    consumer = KafkaConsumer(bootstrap_servers=args.bootstrap_servers, enable_auto_commit=False,
                             request_timeout_ms=30000)
    try:
        partition_ids = consumer.partitions_for_topic(args.topic)
        if not partition_ids:
            raise SystemExit(f"Topic {args.topic!r} does not exist or has no partitions")
        partitions = [TopicPartition(args.topic, p) for p in sorted(partition_ids)]
        consumer.assign(partitions)
        ends = consumer.end_offsets(partitions)
        beginnings = consumer.beginning_offsets(partitions)
        for partition in partitions:
            consumer.seek(partition, max(beginnings[partition], ends[partition] - args.count))
        records = []
        deadline = time.monotonic() + 30
        while any(consumer.position(p) < ends[p] for p in partitions):
            batches = consumer.poll(timeout_ms=1000)
            for partition, batch in batches.items():
                records.extend(record for record in batch if record.offset < ends[partition])
            if time.monotonic() > deadline:
                raise TimeoutError("Timed out reading the topic snapshot")
        if len(partitions) == 1:
            records.sort(key=lambda r: r.offset, reverse=True)
        else:
            records.sort(key=lambda r: (r.timestamp, r.partition, r.offset), reverse=True)
        for record in records[:args.count]:
            print(json.dumps({"partition": record.partition, "offset": record.offset,
                              "kafka_timestamp_ms": record.timestamp,
                              "event": json.loads(record.value)}, ensure_ascii=False))
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
