# CS4371 Group Project 1 environment

The Bluesky producer and a recent-event reader are described in [README.md](README.md).
The entity-counting query, classifier, Logstash project pipeline, and Kibana
visualization have not been built; AG News has not been downloaded or processed.

## Installed tools

| Requirement | Installed setup |
| --- | --- |
| Python | Existing Python 3.10.12, isolated in `.venv` |
| Java for Spark | Existing OpenJDK 11 |
| Apache Spark | PySpark 3.5.8, including Structured Streaming and MLlib |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8`, dependencies cached in `.cache/ivy` |
| Apache Kafka | Official `apache/kafka:3.9.1` Docker image, single-node KRaft; no ZooKeeper needed |
| Elasticsearch / Kibana / Logstash | Official Elastic 8.19.24 Docker images |
| Bluesky connection | `websockets` for the public Jetstream JSON WebSocket |
| Python Kafka client | `kafka-python` |
| Named entity recognition | spaCy and the pretrained English `en_core_web_sm` 3.8.0 model |
| Data and numeric support | NumPy, SciPy, pandas, PyArrow |
| Dataset downloads | `huggingface-hub` (includes the `hf` command) |
| Local configuration | `python-dotenv`; `.env` initialized from `.env.example` |
| Notebooks | JupyterLab and a local `Python (CS4371)` kernel |

`requirements.txt` records the direct Python dependencies. `requirements.lock.txt`
records the exact installed versions, including transitive dependencies and the NLP model.
Spark's Kafka connector is a JVM dependency configured separately in `config/spark`.

## Use the environment

From this directory, in Bash:

```bash
source scripts/activate.sh
docker compose up -d --wait --wait-timeout 360
```

The activation script selects this virtual environment, the existing Java runtime,
local Spark settings, and workspace-local caches. Use it in each new terminal. It
resolves workspace paths at runtime. Spark runs locally with two worker threads,
so a separate Spark master/worker installation is unnecessary for local development.
The Kafka and Elastic containers include their own Java runtimes.

The services were started and verified during setup:

| Service | Host address |
| --- | --- |
| Kafka | `localhost:9092` |
| Elasticsearch | <http://localhost:9200> |
| Kibana | <http://localhost:5601> |
| Logstash status API | <http://localhost:9600> |

Inside the Compose network, Kafka clients must use `kafka:29092` and Elasticsearch
clients must use `http://elasticsearch:9200`. The separate Kafka listeners allow
both host-based Python/Spark and container-based Logstash to connect correctly.

Ports are bound to localhost. Elasticsearch security is disabled for this local
development setup, so Kibana needs no login. Elasticsearch memory mapping is
disabled to work with the existing WSL kernel settings. The service memory limits
leave room for local Spark on this machine. Logstash currently runs the image's
default Beats-to-console pipeline; its project-specific Kafka-to-Elasticsearch
pipeline remains to be written. Kafka topic auto-creation is disabled and no
project topics are created by the producer as needed.

Check or stop services:

```bash
docker compose ps
docker compose logs --tail=50
docker compose stop
```

`docker compose stop` frees the service memory and preserves data. Kafka and
Elasticsearch data use named Docker volumes; `docker compose down` also preserves
those volumes. Avoid `docker compose down -v` unless you intend to delete them.

Optional notebook interface, after activation:

```bash
jupyter lab --ip=127.0.0.1 --no-browser
```

Use the token-bearing URL printed in the terminal and select `Python (CS4371)`.
In an editor, choose `.venv/bin/python` as the Python interpreter.

## Bluesky choice

`.env` contains this public endpoint:

```text
wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post
```

This is the legacy Jetstream API matching the assignment's `commit.record.text`
message format. It requires no account or API key. Newer Jetstream endpoints use
a different envelope, so their examples should not be mixed with this endpoint.
The setup check only opens and closes the WebSocket; it does not read or save posts.
The producer uses the current Jetstream v2 endpoint instead, as documented in
README.md. `.env` is ignored by Git.

## Verify the setup

After activation, with the services running:

```bash
python -m pip check
python scripts/check_environment.py --services --bluesky
```

This checks Python imports, the installed English NLP model, a tiny synthetic Spark
calculation with Python workers, MLlib imports, Kafka connector class loading,
Kafka metadata, Elastic service APIs, and a Bluesky WebSocket handshake. It does
not create topics, ingest data, run a streaming query, fit a model, or create
assignment output. Omit `--services` or `--bluesky` to skip those network checks.
Spark's initial connector resolution needs internet access; its downloaded JARs
are already cached here. The environment smoke-check log is in
`.cache/environment-check.log`.

## Recreate the Python environment

On another Linux machine with Python 3.10, Java 11 or 17, Docker, and Docker Compose:

```bash
python3.10 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.lock.txt
source scripts/activate.sh
python -m ipykernel install --prefix "$VIRTUAL_ENV" --name cs4371 --display-name 'Python (CS4371)'
cp .env.example .env
docker compose pull
docker compose up -d --wait --wait-timeout 360
python scripts/check_environment.py --services --bluesky
```

## References

- [Spark 3.5.8 runtime requirements](https://spark.apache.org/docs/3.5.8/index.html)
- [Spark Structured Streaming Kafka connector](https://spark.apache.org/docs/3.5.8/structured-streaming-kafka-integration.html)
- [Apache Kafka quickstart](https://kafka.apache.org/quickstart/)
- [Public legacy Bluesky Jetstream instances and message format](https://github.com/bluesky-social/jetstream-legacy)
- [Current Bluesky Jetstream API](https://bsky.network/docs/jetstream/)
- [Elastic Docker installation](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/docker.html)
- [Elasticsearch memory mapping setting](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/index-modules-store.html)
- [spaCy pretrained models](https://spacy.io/usage/models)
- [AG News dataset for later project work](https://huggingface.co/datasets/fancyzhx/ag_news)

The project document requires disclosure of AI assistance and full chat transcripts
as PDFs in the final submission. Keep this environment-setup conversation for that purpose.
