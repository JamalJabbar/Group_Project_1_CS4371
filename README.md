# Group_Project_1_CS4371

## Development environment

See [SETUP.md](SETUP.md) for the Python dependencies, local Kafka and Elastic
services, Bluesky Jetstream configuration, and setup verification commands.
For a fresh clone, follow **Recreate the Python environment** before activation.
The current implementation covers Bluesky ingestion into Kafka `topic1`.

## Bluesky to Kafka

From this directory, activate the environment and start only Kafka:

```bash
source scripts/activate.sh
docker compose up -d --wait kafka
python src/jetstream_to_kafka.py
```

The producer creates `topic1` if needed and runs until Ctrl+C. It subscribes to
the current Jetstream v2 endpoint and stores complete JSON envelopes for new
posts with nonempty text and `en` in their `langs` field. Language tags are
provided by the post author and may not always match the text. The text is at
`payload.record.text`; the older
`commit.record.text` format in the assignment belongs to the legacy API.
Previously stored records are retained, including any from before the English
filter was enabled. For a bounded test, use `--max-events 15`.
The Kafka broker comes from `KAFKA_BOOTSTRAP_SERVERS` in the environment or
`.env`, and defaults to `localhost:9092`. `--bootstrap-servers`, `--topic`, and
`--url` override the defaults. The older `BLUESKY_JETSTREAM_URL` in the existing
`.env` is for the legacy API and is not used by this producer.

Read the latest ten stored events, newest first, as JSON Lines:

```bash
python src/recent_events.py --count 10
```

The reader snapshots Kafka's end offsets. `topic1` is created with one partition,
so its offset gives append order. With multiple existing partitions, records are
merged by Kafka timestamp, since Kafka does not define a global partition order.
The producer waits for Kafka acknowledgement before advancing its in-memory
Jetstream cursor. Reconnects can replay the last event, so duplicates are possible;
the cursor is not persisted across process restarts. Kafka retention settings
determine how long the stored posts remain available.

## 1 Spark Streaming with Real Time Data and Kafka (Jamal due Tuesday 11/10 11:59pm)

# 2 Text Classification of Streamed Text with PySpark
## 2.1 Loading Data (Zainab - Wednesday 11/11 by 11:59pm)

## 2.2 Building the Pipeline (Dana - Friday 11/13 by 11:59pm)

## 2.3 Classifying your Streamed Data (Ayesha - Sunday 11/15 by 12:00pm)
