"""Part 2.2: build the text-classification Pipeline and fit it on AG News.

Stages: RegexTokenizer -> StopWordsRemover -> HashingTF -> IDF -> LogisticRegression.
The pipeline is first fit on a training split and scored on a held-out split, then
refit on all 120,000 articles. The final PipelineModel is saved to --model-path so
Part 2.3 applies exactly the same steps to the streamed posts.
"""
import argparse
import json
from pathlib import Path

from dotenv import load_dotenv
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import IDF, HashingTF, RegexTokenizer, StopWordsRemover
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F

from load_data import CLASS_NAMES, ag_news_path, load_ag_news

SANITY_CHECKS = [
    "Lakers beat the Celtics in overtime as LeBron scores 40 points",
    "Stocks fall as the Federal Reserve signals more interest rate hikes",
    "Apple unveils a new chip and an update to its iPhone software",
    "Peace talks resume as the United Nations calls for a ceasefire",
]


def build_pipeline(args):
    """Raw text -> predicted class. Every stage reads the previous stage's output column."""
    tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern=r"[^a-z0-9']+",
                               toLowercase=True, minTokenLength=2)
    stopwords = StopWordsRemover(inputCol="words", outputCol="filtered")
    tf = HashingTF(inputCol="filtered", outputCol="tf", numFeatures=args.num_features)
    idf = IDF(inputCol="tf", outputCol="features", minDocFreq=args.min_doc_freq)
    lr = LogisticRegression(featuresCol="features", labelCol="label", family="multinomial",
                            maxIter=args.max_iter, regParam=args.reg_param,
                            elasticNetParam=args.elastic_net)
    return Pipeline(stages=[tokenizer, stopwords, tf, idf, lr])


def evaluate(predictions):
    """Accuracy, weighted F1 and a confusion matrix (rows: true class, columns: predicted)."""
    metrics = {name: MulticlassClassificationEvaluator(labelCol="label", metricName=name).evaluate(predictions)
               for name in ("accuracy", "f1")}
    cells = {(r["label"], int(r["prediction"])): r["count"]
             for r in predictions.groupBy("label", "prediction").count().collect()}
    metrics["confusion_matrix"] = {
        CLASS_NAMES[t]: {CLASS_NAMES[p]: cells.get((t, p), 0) for p in range(len(CLASS_NAMES))}
        for t in range(len(CLASS_NAMES))}
    return metrics


def print_confusion(matrix):
    width = max(len(name) for name in CLASS_NAMES) + 2
    print("true \\ predicted".ljust(width + 6) + "".join(n.rjust(width) for n in CLASS_NAMES))
    for true_name, row in matrix.items():
        print(true_name.ljust(width + 6) + "".join(str(row[n]).rjust(width) for n in CLASS_NAMES))


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Part 2.2: fit the AG News text classifier and save it.")
    parser.add_argument("--ag-news", help="AG News training Parquet file; downloaded from Hugging Face if omitted")
    parser.add_argument("--model-path", default="models/ag_news_lr", help="where the fitted PipelineModel is saved")
    parser.add_argument("--output-dir", default="outputs/part2_2", help="where metrics.json is written")
    parser.add_argument("--num-features", type=int, default=1 << 18, help="HashingTF buckets")
    parser.add_argument("--min-doc-freq", type=int, default=5, help="IDF minimum document frequency")
    parser.add_argument("--max-iter", type=int, default=100)
    parser.add_argument("--reg-param", type=float, default=0.01)
    parser.add_argument("--elastic-net", type=float, default=0.0, help="0 = L2, 1 = L1")
    parser.add_argument("--test-fraction", type=float, default=0.2,
                        help="held out for evaluation before the final fit; 0 skips evaluation")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if not 0 <= args.test_fraction < 1:
        parser.error("--test-fraction must be at least 0 and less than 1")

    spark = SparkSession.builder.appName("TrainPipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    ag_news = load_ag_news(spark, ag_news_path(args.ag_news)).select("text", "label").cache()
    print(f"Loaded {ag_news.count()} AG News training articles")
    pipeline = build_pipeline(args)
    summary = {"parameters": {k: v for k, v in vars(args).items() if k not in ("ag_news", "output_dir")}}

    if args.test_fraction > 0:
        train, test = ag_news.randomSplit([1 - args.test_fraction, args.test_fraction], seed=args.seed)
        print(f"Fitting on {train.count()} articles, evaluating on {test.count()} held-out articles...")
        metrics = evaluate(pipeline.fit(train).transform(test))
        print(f"\nHeld-out accuracy: {metrics['accuracy']:.4f}   weighted F1: {metrics['f1']:.4f}\n")
        print_confusion(metrics["confusion_matrix"])
        summary["held_out"] = metrics

    print(f"\nFitting the final model on all {ag_news.count()} articles...")
    model = pipeline.fit(ag_news)
    model.write().overwrite().save(args.model_path)
    print(f"Saved PipelineModel to {args.model_path}")

    # Reload from disk so the check uses exactly what Part 2.3 will load.
    reloaded = PipelineModel.load(args.model_path)
    names = F.create_map(*[x for i, name in enumerate(CLASS_NAMES) for x in (F.lit(i), F.lit(name))])
    sample = (reloaded.transform(spark.createDataFrame([(t,) for t in SANITY_CHECKS], ["text"]))
              .select("text", names[F.col("prediction").cast("int")].alias("predicted"),
                      F.round(F.array_max(vector_to_array("probability")), 3).alias("probability")))
    print("\nSanity check with the reloaded model:")
    sample.show(truncate=70)
    summary["sanity_check"] = [row.asDict() for row in sample.collect()]
    summary["model_path"] = args.model_path
    summary["classes"] = dict(enumerate(CLASS_NAMES))

    (out / "metrics.json").write_text(json.dumps(summary, indent=2))
    print(f"Saved metrics to {out / 'metrics.json'}")
    spark.stop()


if __name__ == "__main__":
    main()
