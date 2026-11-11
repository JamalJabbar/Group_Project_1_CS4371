# Group_Project_1_CS4371

## Development environment

See [SETUP.md](SETUP.md) for the Python dependencies, local Kafka and Elastic
services, Bluesky Jetstream configuration, and setup verification commands.
For a fresh clone, follow **Recreate the Python environment** before activation.
Part 1 streams Bluesky posts through Kafka and Spark, indexes named-entity
counts with Logstash and Elasticsearch, and visualizes them in Kibana.

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
filter was enabled. For a bounded test, use `--max-events 15`. Use
`--sample-rate 0.02` to retain approximately 2% when the stream is too busy.
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
python src/entity_counts.py --checkpoint checkpoints/named_entities --trigger "10 seconds"
```

The job reads `topic1` from the earliest available offset on its first run,
extracts named people, organizations, places and related entity types from
English-tagged posts with spaCy's `en_core_web_sm` model, and counts mentions
case-insensitively. Numbers and dates are excluded; model predictions may still
be inaccurate. It creates `topic2` if needed and writes
one JSON message per entity, such as
`{"entity":"london","count":3,"run_id":"main"}`.
Each trigger writes the complete current set of counts, keyed by entity. Counts
are cumulative snapshots: downstream consumers should use the latest value for
each key, not sum messages across triggers. spaCy's predictions may be imperfect.
Spark's Kafka sink provides at-least-once delivery, so output messages may be
duplicated after a failure; treating them as keyed snapshots avoids double-counting.
Idle intervals may not produce a new snapshot when there is no input to process.

The checkpoint preserves Kafka offsets and running state across restarts; keep
the same checkpoint to resume. `--starting-offsets latest` affects only a new
checkpoint. A new checkpoint starts a new count and can write lower values to an
existing output topic. Use a new `--run-id` for a new checkpoint so its output
goes to a separate Elasticsearch index and does not mix with an earlier run.
Set `--input-topic`, `--output-topic`, and
`--bootstrap-servers` to change the Kafka configuration. Use a separate checkpoint
for each independent query and keep the producer running for new posts.

## Visualize the counts

Start all services and create the chart, then run the producer and Spark query
in separate terminals as above:

```bash
docker compose up -d --wait --wait-timeout 360
source scripts/activate.sh
python src/kibana_chart.py --run-id main
```

Open <http://localhost:5601/app/visualize#/edit/cs4371-top-entities-main> and
refresh to see updated counts. Logstash reads `topic2` and indexes one document
per entity and run ID. Each new count replaces the earlier count, so cumulative
snapshots are not added together. `main` is the default run ID. If using a
different `--run-id` for Spark, pass the same ID to `kibana_chart.py`.

To save a plot and its underlying JSON for a report, run:

```bash
python src/plot_entities.py --run-id main --output outputs/top-entities.png
```

Part 1 asks for plots at several intervals; save separate files at the chosen
times, such as 15, 30, 45 and 60 minutes, and include them with the project.
The report also needs a data-source description and a brief interpretation.

## Five-minute test

With all services running and no other producers writing to `topic1`, run:

```bash
source scripts/activate.sh
python src/test_five_minutes.py
```

The test starts a new Spark checkpoint at the current Kafka offset, collects a
2% sample of English-tagged posts for 300 seconds, waits for Spark and Logstash
to catch up, compares the Kafka and Elasticsearch counts, and creates a Kibana
chart plus a PNG and JSON data file under `outputs/<run-id>/`. The generated
run ID keeps this test separate from other counts. `--seconds`, `--sample-rate`,
and `--output-dir` can be changed for other tests. Run this from the project
directory. The five-minute plot produced during development is
`outputs/top-entities-5-minutes.png`; it uses a five-minute sample collected
from 6:28:31 to 6:33:31 p.m. Central time on November 10, 2026. That sample
was reprocessed with the refined named-entity filter. A screenshot from Kibana
is saved as `outputs/kibana-5-minutes.png`; the accompanying JSON and test
metadata are saved alongside the plot.

## 1 Spark Streaming with Real Time Data and Kafka (Jamal - Tuesday 11/10 by 11:59pm)

# 2 Text Classification of Streamed Text with PySpark
## 2.1 Loading Data (Zainab - Wednesday 11/11 by 11:59pm)
### What it does
`src/load_data.py` loads the two datasets used in Part 2:

1. **AG News training set**: downloaded from Hugging Face
   (`fancyzhx/ag_news`, `data/train-00000-of-00001.parquet`) into the local cache
   and loaded into a Spark DataFrame. The script reports the number of articles per class.
2. **Streamed Bluesky posts from Part 1**: every message in `topic1` is read with a
   batch Kafka read (`spark.read.format("kafka")`, `startingOffsets=earliest`,
   `endingOffsets=latest`), and only the post text (`payload.record.text`) is kept.
   Non-English posts are dropped, whitespace is normalized, and empty and duplicate
   messages are removed. The count is reported after each step, with a warning if fewer
   than 1,000 messages remain.

### How to run
With the services running (`docker compose up -d --wait`) and `topic1` holding posts
from `src/jetstream_to_kafka.py`:

```bash
source scripts/activate.sh
python src/load_data.py
```

Options (no paths are hardcoded):
| Option | Default | Purpose |
| --- | --- | --- |
| `--ag-news <path>` | download from Hugging Face | Use a local AG News training Parquet file |
| `--bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` or `localhost:9092` | Kafka broker |
| `--topic` | `topic1` | Topic holding the streamed posts |
| `--input-file <path>` | none | Read posts from a JSON Lines file instead of Kafka, e.g. from `recent_events.py` |
| `--all-languages` | off | Keep posts not tagged as English |
| `--min-messages` | `1000` | Warn if fewer messages remain |
| `--output-dir` | `outputs/part2_1` | Where outputs are written |

### Outputs
Written to `--output-dir`:
- `ag_news_train/`: Parquet with the columns `text`, `label` (0–3) and `category`
- `streamed_messages/`: Parquet with the columns `message_id` and `text`, the cleaned
  posts used in 2.2 and 2.3
- `summary.json`: the class counts and cleaning counts for the report

Other scripts can import `load_ag_news`, `read_raw_posts` and `clean_posts` from
`src/load_data.py`.

### Results (run on November 11, 2026)
| AG News class | Articles |
| --- | --- |
| World | 30,000 |
| Sports | 30,000 |
| Business | 30,000 |
| Sci/Tech | 30,000 |
| **Total** | **120,000** |

| Streamed messages | Count |
| --- | --- |
| Raw messages in `topic1` | 1,335 |
| English-tagged | 1,335 |
| After removing empty messages | 1,335 |
| After removing duplicates | **1,298** |

The AG News training set is balanced across the four classes. All 1,335 streamed
posts were tagged English, because the Part 1 producer already filters by language,
and none were empty. Removing 37 duplicates left 1,298 messages, above the
1,000-message requirement.


## 2.2 Building the Pipeline (Dana - Friday 11/13 by 11:59pm)

## 2.3 Classifying your Streamed Data (Ayesha - Sunday 11/15 by 12:00pm)
