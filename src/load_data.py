"""Part 2.1: load AG News and the Bluesky posts streamed in Part 1.

Reports the number of AG News articles per class, then reads every message in
topic1 with a batch Kafka read (or from a JSON Lines file), keeps only the post
text, removes empty and duplicate messages, and reports how many remain. The
cleaned data is saved as Parquet so Parts 2.2 and 2.3 can reuse it.
"""
import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F, types as T

AG_NEWS_REPO = "fancyzhx/ag_news"
AG_NEWS_TRAIN_FILE = "data/train-00000-of-00001.parquet"
CLASS_NAMES = ["World", "Sports", "Business", "Sci/Tech"]

# Same envelope as src/entity_counts.py: Jetstream v2 puts the text at payload.record.text.
POST_SCHEMA = T.StructType([T.StructField("payload", T.StructType([
    T.StructField("record", T.StructType([
        T.StructField("text", T.StringType()),
        T.StructField("langs", T.ArrayType(T.StringType())),
    ])),
]))])


def ag_news_path(path):
    """Return a local path to the AG News training Parquet, downloading it if none is given."""
    if path:
        return path
    from huggingface_hub import hf_hub_download
    return hf_hub_download(repo_id=AG_NEWS_REPO, filename=AG_NEWS_TRAIN_FILE, repo_type="dataset")


def load_ag_news(spark, path):
    """AG News training data with columns text, label (0-3) and category (class name)."""
    names = F.create_map(*[x for i, name in enumerate(CLASS_NAMES) for x in (F.lit(i), F.lit(name))])
    return (spark.read.parquet(path)
            .select("text", F.col("label").cast("int").alias("label"))
            .withColumn("category", names[F.col("label")]))


def read_raw_posts(spark, args):
    """Every stored event parsed to (text, langs), from Kafka or from a JSON Lines file."""
    if args.input_file:
        # Accept raw Jetstream envelopes or the {"event": ...} lines printed by recent_events.py.
        raw = spark.read.text(args.input_file).select(F.col("value").alias("json"))
        wrapped = F.get_json_object("json", "$.event")
        raw = raw.select(F.coalesce(wrapped, F.col("json")).alias("json"))
    else:
        raw = (spark.read.format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap_servers)
               .option("subscribe", args.topic)
               .option("startingOffsets", "earliest")
               .option("endingOffsets", "latest")
               .load()
               .select(F.col("value").cast("string").alias("json")))
    return raw.select(F.from_json("json", POST_SCHEMA).alias("e")).select("e.payload.record.*")


def clean_posts(posts, english_only=True):
    """Keep only message text; drop empty and duplicate messages. Returns (dataframe, counts)."""
    counts = {"raw_messages": posts.count()}
    if english_only:
        # Part 1's producer only stores English-tagged posts, but older records may predate that filter.
        posts = posts.where(F.array_contains("langs", "en"))
        counts["english_tagged"] = posts.count()
    text = posts.select(F.trim(F.regexp_replace("text", r"\s+", " ")).alias("text"))
    nonempty = text.where(F.col("text").isNotNull() & (F.length("text") > 0))
    counts["after_removing_empty"] = nonempty.count()
    unique = nonempty.dropDuplicates(["text"]).withColumn("message_id", F.monotonically_increasing_id())
    unique = unique.select("message_id", "text").cache()
    counts["after_removing_duplicates"] = unique.count()
    return unique, counts


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Part 2.1: load AG News and the streamed Bluesky posts.")
    parser.add_argument("--ag-news", help="AG News training Parquet file; downloaded from Hugging Face if omitted")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default="topic1")
    parser.add_argument("--input-file", help="read posts from a JSON Lines file instead of Kafka")
    parser.add_argument("--all-languages", action="store_true", help="keep posts not tagged as English")
    parser.add_argument("--min-messages", type=int, default=1000)
    parser.add_argument("--output-dir", default="outputs/part2_1",
                        help="where the cleaned Parquet data and summary JSON are written")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("LoadData").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # --- AG News ---
    ag_news = load_ag_news(spark, ag_news_path(args.ag_news))
    per_class = ag_news.groupBy("label", "category").count().orderBy("label")
    print("\nAG News training articles per class:")
    per_class.show(truncate=False)
    class_counts = {row["category"]: row["count"] for row in per_class.collect()}
    print(f"Total AG News training articles: {sum(class_counts.values())}")
    ag_news.write.mode("overwrite").parquet(str(out / "ag_news_train"))

    # --- Streamed posts ---
    posts, counts = clean_posts(read_raw_posts(spark, args), english_only=not args.all_languages)
    source = args.input_file or f"Kafka topic {args.topic} at {args.bootstrap_servers}"
    print(f"\nStreamed messages from {source}:")
    for name, value in counts.items():
        print(f"  {name.replace('_', ' '):<28}{value}")
    remaining = counts["after_removing_duplicates"]
    if remaining < args.min_messages:
        print(f"WARNING: only {remaining} messages remain; the assignment needs at least "
              f"{args.min_messages}. Keep the producer running longer.")
    print("\nSample of cleaned messages:")
    posts.show(5, truncate=100)
    posts.write.mode("overwrite").parquet(str(out / "streamed_messages"))

    summary = {"ag_news_per_class": class_counts, "ag_news_total": sum(class_counts.values()),
               "streamed_source": source, "streamed_counts": counts,
               "meets_minimum": remaining >= args.min_messages}
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(f"\nSaved cleaned data and summary.json to {out}/")
    spark.stop()


if __name__ == "__main__":
    main()
