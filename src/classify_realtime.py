"""Part 2.3(c): classify Kafka posts and maintain running class counts."""
import argparse
import os
import signal
from threading import Event

from dotenv import load_dotenv
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, functions as F, types as T

from load_data import CLASS_NAMES, POST_SCHEMA


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Classify incoming Kafka posts and show running class counts."
    )
    parser.add_argument("--bootstrap-servers",
                        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--input-topic", default="topic1")
    parser.add_argument("--model-path", default="models/ag_news_lr")
    parser.add_argument("--checkpoint", default="checkpoints/text_classifier")
    parser.add_argument("--starting-offsets", default="latest",
                        help="latest, earliest, or Kafka offsets as JSON")
    parser.add_argument("--trigger", default="10 seconds")
    args = parser.parse_args()

    if not os.path.exists(args.model_path):
        parser.error(f"model path does not exist: {args.model_path}; run train_pipeline.py first")

    spark = SparkSession.builder.appName("BlueskyTextClassification").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    stopping = Event()
    signal.signal(signal.SIGINT, lambda *_: stopping.set())

    model = PipelineModel.load(args.model_path)
    posts = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", args.bootstrap_servers)
             .option("subscribe", args.input_topic)
             .option("startingOffsets", args.starting_offsets)
             .load()
             .select(F.from_json(F.col("value").cast("string"), POST_SCHEMA).alias("event"))
             .select("event.payload.record.*")
             .where(F.col("text").isNotNull() & F.array_contains("langs", "en"))
             .withColumn("text", F.trim(F.regexp_replace("text", r"\s+", " ")))
             .where(F.length("text") > 0)
             .select("text"))

    names = F.create_map(*[x for i, name in enumerate(CLASS_NAMES)
                           for x in (F.lit(i), F.lit(name))])
    predictions = (model.transform(posts)
                   .withColumn("predicted", names[F.col("prediction").cast("int")])
                   .select("predicted"))
    counts = predictions.groupBy("predicted").count()
    query = (counts.writeStream.format("console")
             .outputMode("complete")
             .option("truncate", "false")
             .option("numRows", len(CLASS_NAMES))
             .option("checkpointLocation", args.checkpoint)
             .trigger(processingTime=args.trigger)
             .start())
    try:
        while not stopping.is_set() and not query.awaitTermination(1):
            pass
    finally:
        if query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
