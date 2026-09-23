# ==============================================================================
# COMP90024 Team 39 collaborative project
# Contributor attribution: see CONTRIBUTIONS.md
#
# Description: Fission function entrypoint for news data cleaning (news_clean_main.py).
# ==============================================================================
#!/usr/bin/env python3
"""Fission function: Incremental GDELT news cleaner.

Reads raw GDELT documents from SOURCE_INDEX and writes cleaned records to
TARGET_INDEX with only the fields: news_id, title, content, location, date,
polarity_scores.

This module is deployable to Fission as a Python function with entrypoint
`main`.
"""

import hashlib
import html
import os
import re
from datetime import datetime, timezone

from elasticsearch import Elasticsearch, helpers
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

SOURCE_INDEX = os.environ.get("SOURCE_INDEX", "social_media_data")
TARGET_INDEX = os.environ.get("TARGET_INDEX", "news_cleaned_data")
STATE_INDEX = os.environ.get("STATE_INDEX", "gdelt_news_clean_state")
WATERMARK_ID = "watermark"
BATCH_SIZE = 500

URL_RE = re.compile(r"https?://\S+|www\.\S+")
HTML_TAG_RE = re.compile(r"<[^<]+?>")
WHITESPACE_RE = re.compile(r"\s+")

analyzer = SentimentIntensityAnalyzer()
es_client = None


def read_fission_secret(key, secret_name="es-secret"):
    paths = [
        f"/secrets/default/{secret_name}/{key}",
        f"/secrets/{secret_name}/{key}",
    ]
    for path in paths:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()

    val = os.environ.get(key)
    if val:
        return val

    raise FileNotFoundError(f"Secret not found: {secret_name}/{key}")



def init_es():
    global es_client
    if es_client:
        return es_client

    es_host = os.environ.get("ES_HOST", "https://elasticsearch-es-http.elastic:9200")
    es_user = read_fission_secret("ES_USER")
    es_pass = read_fission_secret("ES_PASS")

    es_client = Elasticsearch(
        [es_host],
        basic_auth=(es_user, es_pass),
        verify_certs=False,
        ssl_show_warn=False,
        request_timeout=120,
    )
    es_client.info()
    return es_client



def clean_text(text):
    if not text:
        return ""
    text = html.unescape(text)
    text = HTML_TAG_RE.sub(" ", text)
    text = URL_RE.sub(" ", text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text



def normalize_date(value):
    if not value:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")

    s = str(value).strip()
    if not s:
        return ""

    s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass

    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    return m.group(1) if m else ""



def get_watermark(es):
    try:
        doc = es.get(index=STATE_INDEX, id=WATERMARK_ID)
        return doc["_source"].get("last_processed_at", "")
    except Exception:
        return ""



def set_watermark(es, ts):
    if not ts:
        return
    if not es.indices.exists(index=STATE_INDEX):
        es.indices.create(index=STATE_INDEX)
    es.index(
        index=STATE_INDEX,
        id=WATERMARK_ID,
        document={
            "last_processed_at": ts,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        refresh=True,
    )



def get_max_news_id(es):
    try:
        res = es.search(
            index=TARGET_INDEX,
            size=0,
            aggs={"max_id": {"max": {"field": "news_id"}}},
        )
        v = res["aggregations"]["max_id"].get("value")
        return int(v) if v else 0
    except Exception:
        return 0



def ensure_target_index(es, full_rebuild=False):
    if full_rebuild and es.indices.exists(index=TARGET_INDEX):
        es.indices.delete(index=TARGET_INDEX)
        print(f"Deleted index {TARGET_INDEX}")

    if not es.indices.exists(index=TARGET_INDEX):
        es.indices.create(
            index=TARGET_INDEX,
            mappings={
                "properties": {
                    "news_id":        {"type": "long"},
                    "title":          {"type": "text"},
                    "content":        {"type": "text"},
                    "location":       {"type": "keyword"},
                    "date":           {"type": "date", "format": "yyyy-MM-dd"},
                    "polarity_scores": {"type": "float"},
                }
            },
        )
        print(f"Created index {TARGET_INDEX}")

    if not es.indices.exists(index=STATE_INDEX):
        es.indices.create(index=STATE_INDEX)
        print(f"Created index {STATE_INDEX}")



def stable_doc_id(raw_key):
    if not raw_key:
        raw_key = "no-key"
    digest = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
    return f"news_{digest}"



def build_source_query(watermark):
    if watermark:
        return {"range": {"search_start_date": {"gt": watermark}}}
    return {"match_all": {}}



def iter_source_docs(es, watermark):
    query = {"query": build_source_query(watermark)}
    return helpers.scan(
        es,
        index=SOURCE_INDEX,
        query=query,
        size=BATCH_SIZE,
        preserve_order=False,
    )



def build_cleaned_doc(source_doc, next_id):
    s = source_doc.get("_source", {})
    title = str(s.get("title", "")).strip()
    content = clean_text(str(s.get("content", "")).strip())
    if not title and not content:
        return None

    combined = f"{title}\n{content}".strip()
    polarity = analyzer.polarity_scores(combined)["compound"]

    location = str(s.get("location", "") or s.get("sourcecountry", "")).strip()
    date = normalize_date(s.get("search_start_date") or s.get("seendate"))
    if not date:
        return None

    raw_key = s.get("post_id") or s.get("url") or title or date
    doc_id = stable_doc_id(str(raw_key))

    cleaned = {
        "news_id":         next_id,
        "title":           title,
        "content":         content,
        "location":        location,
        "date":            date,
        "polarity_scores": round(polarity, 4),
    }
    return doc_id, cleaned



def process_documents(es, watermark):
    counter = get_max_news_id(es)
    max_ts = ""
    actions = []

    for hit in iter_source_docs(es, watermark):
        source = hit.get("_source", {})
        timestamp = str(source.get("search_start_date", "") or "")
        if timestamp and timestamp > max_ts:
            max_ts = timestamp

        candidate = build_cleaned_doc(hit, counter + 1)
        if not candidate:
            continue

        doc_id, cleaned = candidate
        counter += 1
        actions.append({
            "_index": TARGET_INDEX,
            "_id":    doc_id,
            "_source": cleaned,
        })

    if not actions:
        return 0, 0, max_ts
    
    success, failed = helpers.bulk(es, actions, stats_only=True)
    return success, failed, max_ts



def fission_main(full=False):
    try:
        es = init_es()
    except Exception as exc:
        return {"error": f"ES init failed: {exc}"}

    ensure_target_index(es, full_rebuild=full)
    watermark = "" if full else get_watermark(es)

    indexed, failed, max_ts = process_documents(es, watermark)
    if max_ts:
        set_watermark(es, max_ts)

    return {
        "status": "success",
        "indexed": indexed,
        "failed": failed,
        "watermark": max_ts or watermark or "(none)",
    }


def main():
    return fission_main()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Clean GDELT raw news documents into reduced news records.")
    parser.add_argument("--full", action="store_true", help="Delete target index and reprocess all raw source data.")
    args = parser.parse_args()

    result = fission_main(full=args.full)
    print(result)
