"""Part 2.3(a-b): classify the cleaned streamed posts with the saved model.

The script applies the PipelineModel produced by train_pipeline.py to the
cleaned streamed messages, writes one prediction per message, reports class
counts, shows examples, and creates a 50-message manual-label template. It can
also evaluate the completed manual-label file for Part 2.3(b).
"""
import argparse
import csv
import json
from pathlib import Path

from dotenv import load_dotenv
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F

from load_data import CLASS_NAMES


def prediction_frame(model, posts):
    """Apply the saved model and add readable labels and predicted probability."""
    names = F.create_map(*[x for i, name in enumerate(CLASS_NAMES)
                           for x in (F.lit(i), F.lit(name))])
    transformed = model.transform(posts)
    probabilities = vector_to_array(F.col("probability"))
    prediction_index = F.col("prediction").cast("int")
    return (transformed
            .withColumn("prediction_index", prediction_index)
            .withColumn("predicted", names[prediction_index])
            .withColumn("predicted_probability", F.array_max(probabilities))
            .select("message_id", "text", "prediction_index", "predicted",
                    "predicted_probability"))


def write_manual_template(rows, path):
    """Write the selected messages for manual labeling outside Spark."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "message_id", "text", "model_prediction", "model_probability",
            "human_label", "correct",
        ])
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "message_id": row["message_id"],
                "text": row["text"],
                "model_prediction": row["predicted"],
                "model_probability": row["predicted_probability"],
                "human_label": "",
                "correct": "",
            })


def evaluate_manual_labels(path, output_path):
    """Evaluate completed manual labels, excluding None of these from accuracy."""
    valid = set(CLASS_NAMES)
    rows = list(csv.DictReader(path.open(encoding="utf-8", newline="")))
    labeled = [row for row in rows if row.get("human_label", "").strip()]
    excluded = [row for row in labeled if row["human_label"].strip() not in valid]
    scored = [row for row in labeled if row["human_label"].strip() in valid]
    correct = [row for row in scored
               if row["human_label"].strip() == row["model_prediction"].strip()]
    result = {
        "file": str(path),
        "total_rows": len(rows),
        "labeled_rows": len(labeled),
        "none_of_these": len(excluded),
        "four_class_rows": len(scored),
        "correct_four_class": len(correct),
        "four_class_accuracy": len(correct) / len(scored) if scored else None,
        "incorrect_examples": [
            {key: row[key] for key in ("message_id", "text", "human_label",
                                        "model_prediction", "model_probability")}
            for row in scored if row not in correct
        ][:10],
    }
    output_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Classify cleaned streamed messages with the saved AG News model."
    )
    parser.add_argument("--input-path", default="outputs/part2_1/streamed_messages",
                        help="cleaned streamed-message Parquet path")
    parser.add_argument("--model-path", default="models/ag_news_lr",
                        help="saved PipelineModel path")
    parser.add_argument("--output-dir", default="outputs/part2_3",
                        help="where classification outputs are written")
    parser.add_argument("--sample-size", type=int, default=50,
                        help="number of messages to put in the manual-label template")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--evaluate-manual-labels", action="store_true",
                        help="evaluate the completed manual_labels.csv file")
    args = parser.parse_args()
    if args.sample_size < 1:
        parser.error("--sample-size must be positive")

    input_path = Path(args.input_path)
    model_path = Path(args.model_path)
    out = Path(args.output_dir)
    if not input_path.exists():
        parser.error(f"input path does not exist: {input_path}")
    if not model_path.exists():
        parser.error(f"model path does not exist: {model_path}; run train_pipeline.py first")
    out.mkdir(parents=True, exist_ok=True)

    manual_path = out / "manual_labels.csv"
    evaluation_path = out / "manual_evaluation.json"
    if args.evaluate_manual_labels:
        if not manual_path.exists():
            parser.error(f"manual label file does not exist: {manual_path}")
        result = evaluate_manual_labels(manual_path, evaluation_path)
        print(json.dumps(result, indent=2))
        return

    spark = SparkSession.builder.appName("ClassifyStreamedMessages").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        posts = spark.read.parquet(str(input_path)).select("message_id", "text")
        model = PipelineModel.load(str(model_path))
        predictions = prediction_frame(model, posts).cache()
        total = predictions.count()

        predictions.write.mode("overwrite").parquet(str(out / "predictions.parquet"))
        predictions.write.mode("overwrite").json(str(out / "predictions.json"))

        counts = {row["predicted"]: row["count"]
                  for row in predictions.groupBy("predicted").count().collect()}
        counts = {name: counts.get(name, 0) for name in CLASS_NAMES}
        (out / "class_counts.json").write_text(
            json.dumps({"total_messages": total, "predicted_counts": counts}, indent=2) + "\n",
            encoding="utf-8",
        )

        examples = {}
        for name in CLASS_NAMES:
            rows = (predictions.where(F.col("predicted") == name)
                    .select("message_id", "text", "predicted", "predicted_probability")
                    .orderBy("message_id").limit(5).collect())
            examples[name] = [row.asDict() for row in rows]
        (out / "examples.json").write_text(
            json.dumps(examples, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )

        sample = (predictions.orderBy(F.rand(args.seed)).limit(args.sample_size)
                  .select("message_id", "text", "predicted", "predicted_probability")
                  .collect())
        write_manual_template([row.asDict() for row in sample], manual_path)

        print(f"Classified {total} streamed messages")
        print("Predicted class counts:")
        for name in CLASS_NAMES:
            print(f"  {name:<10}{counts[name]}")
        print(f"Saved predictions and examples to {out}/")
        print(f"Complete human labels in {manual_path}, then run:")
        print(f"  python src/classify_streamed.py --output-dir {out} --evaluate-manual-labels")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
