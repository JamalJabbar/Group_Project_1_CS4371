import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
import requests


def wait_for(check, seconds, message):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(1)
    raise RuntimeError(message)


def stop(process):
    if process and process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=60)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise RuntimeError("Process did not stop cleanly")


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Collect and plot a fresh five-minute sample.")
    parser.add_argument("--seconds", type=int, default=300)
    parser.add_argument("--sample-rate", type=float, default=0.02)
    parser.add_argument("--output-dir", default="outputs")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--elasticsearch-url", default=os.getenv("ELASTICSEARCH_URL", "http://localhost:9200"))
    args = parser.parse_args()
    if args.seconds < 1 or not 0 < args.sample_rate <= 1:
        parser.error("seconds must be positive and sample rate must be between 0 and 1")
    if not os.getenv("SPARK_CONF_DIR"):
        parser.error("activate the project first with: source scripts/activate.sh")

    run_id = f"test-{uuid.uuid4().hex[:12]}"
    directory = Path(args.output_dir) / run_id
    directory.mkdir(parents=True)
    checkpoint = directory / "checkpoint"
    source = TopicPartition("topic1", 0)
    sink = TopicPartition("topic2", 0)
    consumer = KafkaConsumer(bootstrap_servers=args.bootstrap_servers, enable_auto_commit=False)
    spark = producer = None
    try:
        with (directory / "spark.log").open("w") as spark_log, (directory / "producer.log").open("w") as producer_log:
            spark = subprocess.Popen([sys.executable, "src/entity_counts.py", "--bootstrap-servers", args.bootstrap_servers,
                                      "--run-id", run_id, "--checkpoint", str(checkpoint),
                                      "--starting-offsets", "latest"], stdout=spark_log, stderr=subprocess.STDOUT)
            def ready():
                if spark.poll() is not None:
                    raise RuntimeError(f"Spark exited; inspect {directory / 'spark.log'}")
                return (checkpoint / "offsets" / "0").exists()
            wait_for(ready, 120, "Spark did not initialize")
            start_offset = json.loads((checkpoint / "offsets" / "0").read_text().splitlines()[-1])["topic1"]["0"]
            output_offset = consumer.end_offsets([sink])[sink]
            started = datetime.now(timezone.utc)
            producer = subprocess.Popen([sys.executable, "src/jetstream_to_kafka.py", "--bootstrap-servers", args.bootstrap_servers,
                                         "--sample-rate", str(args.sample_rate)], stdout=producer_log, stderr=subprocess.STDOUT)
            try:
                producer.wait(timeout=args.seconds)
                raise RuntimeError(f"Producer exited early; inspect {directory / 'producer.log'}")
            except subprocess.TimeoutExpired:
                stop(producer)
            ended = datetime.now(timezone.utc)
            end_offset = consumer.end_offsets([source])[source]
            if end_offset <= start_offset:
                raise RuntimeError("No posts were collected")

            def processed():
                if spark.poll() is not None:
                    raise RuntimeError(f"Spark exited; inspect {directory / 'spark.log'}")
                files = [p for p in (checkpoint / "commits").iterdir() if p.name.isdecimal()]
                if not files:
                    return False
                batch = max(int(p.name) for p in files)
                offsets = json.loads((checkpoint / "offsets" / str(batch)).read_text().splitlines()[-1])
                return offsets["topic1"]["0"] >= end_offset
            wait_for(processed, 180, "Spark did not finish processing the sample")
            stop(spark)

        end_output = consumer.end_offsets([sink])[sink]
        consumer.assign([sink])
        consumer.seek(sink, output_offset)
        expected = {}
        while consumer.position(sink) < end_output:
            for message in consumer.poll(timeout_ms=1000).get(sink, []):
                if message.offset >= end_output:
                    continue
                item = json.loads(message.value)
                if item.get("run_id") == run_id:
                    expected[item["entity"]] = item["count"]
        if not expected:
            raise RuntimeError("The sample produced no entity counts")

        def indexed():
            response = requests.get(f"{args.elasticsearch_url}/cs4371-entities-{run_id}/_search",
                                    json={"size": 10000}, timeout=15)
            if response.status_code == 404:
                return False
            response.raise_for_status()
            hits = response.json()["hits"]["hits"]
            return {hit["_source"]["entity"]: hit["_source"]["count"] for hit in hits} == expected
        wait_for(indexed, 180, "Elasticsearch did not catch up with topic2")
        subprocess.run([sys.executable, "src/kibana_chart.py", "--run-id", run_id], check=True)
        plot = directory / "top-entities.png"
        subprocess.run([sys.executable, "src/plot_entities.py", "--run-id", run_id,
                        "--elasticsearch-url", args.elasticsearch_url, "--output", str(plot),
                        "--title", f"Top 10 named entities after {args.seconds} seconds"], check=True)
        metadata = {"run_id": run_id, "started_utc": started.isoformat(), "ended_utc": ended.isoformat(),
                    "sample_rate": args.sample_rate, "topic1_start_offset": start_offset,
                    "topic1_end_offset": end_offset, "posts": end_offset - start_offset,
                    "unique_entities": len(expected)}
        (directory / "test.json").write_text(json.dumps(metadata, indent=2) + "\n")
        print(f"Verified {metadata['posts']} posts and {len(expected)} counts. Results: {directory}")
    finally:
        stop(producer)
        stop(spark)
        consumer.close()


if __name__ == "__main__":
    main()
