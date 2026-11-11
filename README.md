# Group_Project_1_CS4371

## Development environment

See [SETUP.md](SETUP.md) for the Python dependencies, local Kafka and Elastic
services, Bluesky Jetstream configuration, and setup verification commands.
For a fresh clone, follow **Recreate the Python environment** before activation.
The current implementation covers Bluesky ingestion into Kafka `topic1` and
running named-entity counts in `topic2`.

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

## Named-entity counts

With Kafka running, start the producer in one terminal and, in another terminal:

```bash
source scripts/activate.sh
python src/entity_counts.py --checkpoint checkpoints/entity_counts --trigger "10 seconds"
```

The job reads `topic1` from the earliest available offset on its first run,
extracts entities from English-tagged posts with spaCy's `en_core_web_sm` model,
and counts mentions case-insensitively. It creates `topic2` if needed and writes
one JSON message per entity, such as `{"entity":"london","count":3}`.
Each trigger writes the complete current set of counts, keyed by entity. Counts
are cumulative snapshots: downstream consumers should use the latest value for
each key, not sum messages across triggers. spaCy's predictions may be imperfect.
Spark's Kafka sink provides at-least-once delivery, so output messages may be
duplicated after a failure; treating them as keyed snapshots avoids double-counting.
Idle intervals may not produce a new snapshot when there is no input to process.

The checkpoint preserves Kafka offsets and running state across restarts; keep
the same checkpoint to resume. `--starting-offsets latest` affects only a new
checkpoint. A new checkpoint starts a new count and can write lower values to an
existing output topic. Set `--input-topic`, `--output-topic`, and
`--bootstrap-servers` to change the Kafka configuration. Use a separate checkpoint
for each independent query and keep the producer running for new posts.

## 1 Spark Streaming with Real Time Data and Kafka (Jamal - Tuesday 11/10 by 11:59pm)

# 2 Text Classification of Streamed Text with PySpark
## 2.1 Loading Data (Zainab - Wednesday 11/11 by 11:59pm)

## 2.2 Building the Pipeline (Dana - Friday 11/13 by 11:59pm)

## 2.3 Classifying your Streamed Data (Ayesha - Sunday 11/15 by 12:00pm)
