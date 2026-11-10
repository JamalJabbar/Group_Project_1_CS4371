"""Check installed tools only; does not collect posts, train models, or create topics."""

import argparse
import asyncio
import importlib
from importlib.metadata import version
import json
import os
from pathlib import Path
from urllib.request import urlopen

from dotenv import load_dotenv


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--services", action="store_true", help="Check running Kafka and Elastic services")
    parser.add_argument("--bluesky", action="store_true", help="Open and close a public WebSocket without consuming posts")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env")

    for module, distribution in [
        ("pyspark", "pyspark"), ("websockets", "websockets"),
        ("kafka", "kafka-python"), ("spacy", "spacy"),
        ("numpy", "numpy"), ("scipy", "scipy"),
        ("pandas", "pandas"), ("pyarrow", "pyarrow"),
        ("huggingface_hub", "huggingface-hub"), ("jupyterlab", "jupyterlab"),
        ("ipykernel", "ipykernel"),
    ]:
        importlib.import_module(module)
        print(f"OK {distribution} {version(distribution)}", flush=True)

    import spacy
    nlp = spacy.load("en_core_web_sm")
    assert "ner" in nlp.pipe_names
    print(f"OK English NER model {nlp.meta['version']}", flush=True)

    from pyspark.sql import SparkSession
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
    from pyspark.ml import Pipeline

    spark = SparkSession.builder.appName("cs4371-environment-check").getOrCreate()
    try:
        spark.sparkContext.setLogLevel("ERROR")
        assert spark.range(3).rdd.map(lambda row: row.id + 1).sum() == 6
        # Class loading verifies both connector directions without running a query.
        loader = spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
        for name in [
            "org.apache.spark.sql.kafka010.KafkaSourceProvider",
            "org.apache.spark.sql.kafka010.KafkaBatchWrite",
            "org.apache.kafka.clients.consumer.KafkaConsumer",
        ]:
            loader.loadClass(name)
        print("OK local Spark Python workers, MLlib imports, and Kafka JVM connector", flush=True)
    finally:
        spark.stop()

    if args.services:
        from kafka.admin import KafkaAdminClient
        admin = KafkaAdminClient(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            request_timeout_ms=15000,
            api_version_auto_timeout_ms=15000,
        )
        try:
            admin.list_topics()
            print("OK Kafka metadata request", flush=True)
        finally:
            admin.close()
        for name, url in [
            ("Elasticsearch", os.getenv("ELASTICSEARCH_URL", "http://localhost:9200") + "/_cluster/health"),
            ("Kibana", os.getenv("KIBANA_URL", "http://localhost:5601") + "/api/status"),
            ("Logstash", "http://localhost:9600/_node/pipelines"),
        ]:
            with urlopen(url, timeout=30) as response:
                data = json.load(response)
            if name == "Elasticsearch":
                assert data["status"] in ("green", "yellow"), data["status"]
            if name == "Kibana":
                assert data["status"]["overall"]["level"] == "available", data["status"]["overall"]
            if name == "Logstash":
                assert data.get("pipelines"), "No running Logstash pipeline"
            print(f"OK {name} API", flush=True)

    if args.bluesky:
        import websockets

        async def handshake():
            url = os.getenv(
                "BLUESKY_JETSTREAM_URL",
                "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
            )
            async with websockets.connect(url, open_timeout=20, close_timeout=5, max_queue=1):
                print("OK Bluesky Jetstream WebSocket handshake (no posts consumed)", flush=True)

        asyncio.run(handshake())


if __name__ == "__main__":
    main()
