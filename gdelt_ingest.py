# ==============================================================================
# COMP90024 Team 39 collaborative project
# Contributor attribution: see CONTRIBUTIONS.md
#
# Description: Script for ingesting news data from sources like GDELT (gdelt_ingest.py).
# ==============================================================================
import os
import ssl
import time
import json
import urllib3
import pandas as pd
from datetime import datetime, timedelta
from newspaper import Article
from gdeltdoc import GdeltDoc, Filters
from elasticsearch import Elasticsearch

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# Configuration
# =========================

ES_INDEX   = "social_media_data"
BATCH_SIZE = 20

KEYWORDS = [
    "petrol prices",
    "fuel excise",
    "fuel tax",
    "gasoline prices",
    "fuel prices",
]

# =========================
# Fission secret reader (mirrors bsky-ingest/fission/main.py)
# =========================

def read_fission_secret(key, secret_name="bsky-secrets"):
    """
    Read a K8s Secret mounted by Fission.
    Falls back to an environment variable with the same name for local testing.
    """
    paths = [
        f"/secrets/default/{secret_name}/{key}",
        f"/secrets/{secret_name}/{key}",
    ]
    for path in paths:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()

    # Local fallback
    val = os.environ.get(key)
    if val:
        return val

    raise FileNotFoundError(
        f"Secret '{key}' not found in Fission paths or env var '{key}'."
    )

# =========================
# Elasticsearch
# =========================

def init_es():
    es_host = os.environ.get("ES_HOST", "https://elasticsearch-es-http.elastic:9200")
    es_user = read_fission_secret("ES_USER")
    es_pass = read_fission_secret("ES_PASS")

    # Explicit ssl_context — required for Python 3.14 to avoid TLS handshake hang
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    es = Elasticsearch(
        [es_host],
        basic_auth=(es_user, es_pass),
        ssl_context=ssl_context,
        request_timeout=60,
        verify_certs=False,
    )
    es.info()   # fail fast if unreachable
    return es

# =========================
# Document formatter
# =========================

def format_document(row, content):
    """Map a GDELT DataFrame row + scraped content → ES document."""
    seen = row.get("seendate", "")
    if hasattr(seen, "isoformat"):
        seen = seen.isoformat()
    else:
        seen = str(seen).strip()

    url    = str(row.get("url", "")).strip()
    doc_id = f"news_{abs(hash(url))}" if url else None

    doc = {
        "platform":          "gdelt",
        "source":            str(row.get("domain", "")).strip(),
        "post_id":           url,
        "title":             str(row.get("title", "")).strip(),
        "content":           content,
        "author":            "",
        "description":       "",
        "location":          "Australia",
        "location_raw":      str(row.get("sourcecountry", "")).strip(),
        "language":          str(row.get("language", "")).strip(),
        "url":               url,
        "keyword":           str(row.get("keyword", "")).strip(),
        "search_start_date": str(row.get("search_start_date", "")).strip(),
        "search_end_date":   str(row.get("search_end_date", "")).strip(),
        "created_at":        seen,
    }
    return doc_id, doc

# =========================
# Data collection (yesterday only)
# =========================

def collect_yesterday(keywords):
    """Fetch GDELT articles for yesterday only."""
    gd        = GdeltDoc()
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    today     = datetime.utcnow().strftime("%Y-%m-%d")

    print(f"Fetching articles for date: {yesterday}")
    all_articles = []

    for kw in keywords:
        print(f"  Keyword: {kw!r}")
        success = False

        for attempt in range(5):
            try:
                f = Filters(
                    keyword=kw,
                    start_date=yesterday,
                    end_date=today,
                    num_records=50,
                )
                articles = gd.article_search(f)

                if articles is not None and len(articles) > 0:
                    articles["keyword"]           = kw
                    articles["search_start_date"] = yesterday
                    articles["search_end_date"]   = today
                    all_articles.append(articles)
                    print(f"    Collected {len(articles)} articles")
                else:
                    print("    No articles found")

                success = True
                break

            except Exception as e:
                err_str = repr(e).lower()
                if "ratelimit" in err_str or "429" in err_str or "too many" in err_str:
                    wait = min(15 * (2 ** attempt), 60)
                    print(f"    Rate limited. Waiting {wait}s (retry {attempt + 1}/5)...")
                    time.sleep(wait)
                else:
                    print(f"    Retry {attempt + 1}/5 failed: {repr(e)}")
                    time.sleep(5)

        if not success:
            print(f"    Skipped: {kw}")

        time.sleep(5)   # base delay to avoid rate limiting

    return all_articles

# =========================
# Data processing & dedup
# =========================

def process_articles(all_articles):
    df = pd.concat(all_articles, ignore_index=True)
    print(f"Before filtering: {len(df)}")

    df = df[
        (df["sourcecountry"] == "Australia") |
        (df["title"].str.contains(
            "Australia|Sydney|Melbourne|NSW|VIC", case=False, na=False
        ))
    ]
    print(f"After AU filter:  {len(df)}")

    df = df.drop_duplicates(subset=["url"])
    print(f"After URL dedup:  {len(df)}")

    df["title_clean"] = df["title"].str.lower().str.strip()
    df = df.drop_duplicates(subset=["title_clean"])
    print(f"After title dedup:{len(df)}")

    df["seendate"] = pd.to_datetime(df["seendate"], errors="coerce")
    df = df.sort_values("seendate").reset_index(drop=True)

    return df

# =========================
# Article content scraping
# =========================

def scrape_contents(df):
    contents = []
    total    = len(df)

    for i, url in enumerate(df["url"]):
        print(f"  Scraping {i + 1}/{total}: {url[:80]}")
        try:
            article = Article(url)
            article.download()
            article.parse()
            contents.append(article.text)
        except Exception as e:
            print(f"  Failed: {e}")
            contents.append("")
        time.sleep(1)

    return contents

# =========================
# Single-document upload
# =========================

def upload_documents(es_client, df):
    es_index      = os.environ.get("ES_INDEX", ES_INDEX)
    success_count = 0
    failed_count  = 0
    total         = len(df)

    print(f"\nUploading {total} documents to index '{es_index}'...")

    for i, (_, row) in enumerate(df.iterrows(), 1):
        content     = row.get("content", "")
        doc_id, doc = format_document(row, content)

        try:
            kwargs = dict(index=es_index, body=doc)
            if doc_id:
                kwargs["id"] = doc_id
            es_client.index(**kwargs)
            success_count += 1
            print(f"  [{i}/{total}] Indexed: {doc_id or 'auto-id'}")
        except Exception as e:
            failed_count += 1
            print(f"  [{i}/{total}] Failed ({doc_id}): {e}")

    print("\n--- Upload complete ---")
    print(f"Success: {success_count} records")
    if failed_count > 0:
        print(f"Failed:  {failed_count} records")

# =========================
# Main
# =========================

def main():
    # 1. Connect to ES
    print("Connecting to Elasticsearch...")
    try:
        es_client = init_es()
        print("ES connection successful!\n")
    except Exception as e:
        print(f"ES connection failed: {e}")
        return {"error": f"ES connection failed: {e}"}, 500

    # 2. Fetch yesterday's articles from GDELT
    all_articles = collect_yesterday(KEYWORDS)
    if not all_articles:
        print("No data collected.")
        return {"status": "success", "message": "No data collected."}

    # 3. Process & deduplicate
    df = process_articles(all_articles)
    if df.empty:
        print("No Australia-related articles after filtering.")
        return {"status": "success", "message": "No Australia-related articles after filtering."}

    # 4. Scrape full article content
    print("\nScraping article contents...")
    df["content"] = scrape_contents(df)

    # 5. Upload documents one by one
    upload_documents(es_client, df)
    
    return {"status": "success", "message": "GDELT ingestion completed."}

if __name__ == "__main__":
    main()
