import argparse
import json
import os
import re
from pathlib import Path

from dotenv import load_dotenv
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import requests


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Save a bar plot of Elasticsearch entity counts.")
    parser.add_argument("--elasticsearch-url", default=os.getenv("ELASTICSEARCH_URL", "http://localhost:9200"))
    parser.add_argument("--run-id", default="main")
    parser.add_argument("--output", default="outputs/top-entities.png")
    parser.add_argument("--title", default="Top 10 named entities")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,49}", args.run_id):
        parser.error("--run-id must contain only lowercase letters, digits and hyphens")

    response = requests.post(f"{args.elasticsearch_url}/cs4371-entities-{args.run_id}/_search",
                             json={"size": 10, "sort": [{"count": "desc"}, {"entity.keyword": "asc"}]}, timeout=30)
    response.raise_for_status()
    rows = [hit["_source"] for hit in response.json()["hits"]["hits"]]
    if not rows:
        raise SystemExit("No entity counts found for this run")
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh([r["entity"] for r in rows][::-1], [r["count"] for r in rows][::-1], color="#006f8c")
    ax.set(title=args.title, xlabel="Mentions", ylabel="Entity")
    ax.bar_label(ax.containers[0], padding=3)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    path.with_suffix(".json").write_text(json.dumps(
        [{"entity": r["entity"], "count": r["count"]} for r in rows], ensure_ascii=False, indent=2) + "\n")
    print(f"Saved {path} and {path.with_suffix('.json')}")


if __name__ == "__main__":
    main()
