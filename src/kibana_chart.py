import argparse
import json
import os
import re

from dotenv import load_dotenv
import requests


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Create a Kibana chart of entity counts.")
    parser.add_argument("--kibana-url", default=os.getenv("KIBANA_URL", "http://localhost:5601"))
    parser.add_argument("--run-id", default="main")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,49}", args.run_id):
        parser.error("--run-id must contain only lowercase letters, digits and hyphens")

    headers = {"kbn-xsrf": "true"}
    view_id = "cs4371-entities-view"
    view = requests.get(f"{args.kibana_url}/api/data_views/data_view/{view_id}", timeout=30)
    if view.status_code == 404:
        view = requests.post(f"{args.kibana_url}/api/data_views/data_view", headers=headers,
                             json={"data_view": {"id": view_id, "title": "cs4371-entities-*", "allowNoIndex": True}},
                             timeout=30)
    view.raise_for_status()
    default = requests.get(f"{args.kibana_url}/api/data_views/default", timeout=30)
    default.raise_for_status()
    if not default.json().get("data_view_id"):
        default = requests.post(f"{args.kibana_url}/api/data_views/default", headers=headers,
                                json={"data_view_id": view_id}, timeout=30)
        default.raise_for_status()

    title = f"Top 10 named entities ({args.run_id})"
    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "title": title,
        "data": {
            "url": {
                "index": f"cs4371-entities-{args.run_id}",
                "body": {"size": 10, "sort": [{"count": "desc"}, {"entity.keyword": "asc"}]}
            },
            "format": {"property": "hits.hits"}
        },
        "mark": {"type": "bar", "color": "#006f8c"},
        "encoding": {
            "y": {"field": "_source.entity", "type": "nominal", "sort": "-x", "title": "Entity"},
            "x": {"field": "_source.count", "type": "quantitative", "title": "Mentions",
                  "axis": {"tickMinStep": 1, "format": "d", "tickCount": 8}},
            "tooltip": [
                {"field": "_source.entity", "type": "nominal", "title": "Entity"},
                {"field": "_source.count", "type": "quantitative", "title": "Mentions"}
            ]
        }
    }
    state = {"title": title, "type": "vega", "params": {"spec": json.dumps(spec)}, "aggs": []}
    obj = {"attributes": {
        "title": title, "visState": json.dumps(state), "uiStateJSON": "{}",
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})}
    }}
    chart_id = f"cs4371-top-entities-{args.run_id}"
    response = requests.post(f"{args.kibana_url}/api/saved_objects/visualization/{chart_id}",
                             params={"overwrite": "true"}, json=obj,
                             headers=headers, timeout=30)
    response.raise_for_status()
    print(f"{args.kibana_url}/app/visualize#/edit/{chart_id}")


if __name__ == "__main__":
    main()
